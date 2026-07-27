"""Smoke test the com.esri filegdb Spark data source against a real File GeoDatabase.

Usage: uv run smoke_test.py [/path/to/some.gdb]
"""

import glob
import os
import struct
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

HERE = os.path.dirname(os.path.abspath(__file__))
# A fixture shipped with the repo, so the no-argument form works for anyone who cloned it.
DEFAULT_GDB = os.path.join(HERE, "data", "Miami.gdb")


def find_jar() -> str:
    jars = glob.glob(os.path.join(HERE, "target", "filegdb-*.jar"))
    jars = [j for j in jars if "sources" not in j and "javadoc" not in j]
    if not jars:
        sys.exit("No filegdb jar in target/ - run 'mvn clean install -DskipTests' first.")
    # Newest, not lexicographically last: target/ accumulates jars across spark profiles and
    # version bumps, and picking the wrong one silently validates code that was not just built.
    return max(jars, key=os.path.getmtime)


def gdb_module(spark: SparkSession):
    """The Scala FileGDB object, through py4j.

    A Scala object with a companion class emits no static forwarders, so the
    MODULE$ singleton has to be reached by name.
    """
    return getattr(getattr(spark.sparkContext._jvm.com.esri.gdb, "FileGDB$"), "MODULE$")


def is_compressed(spark: SparkSession, gdb: str, name: str) -> bool:
    """A .gdbtable signature other than 3 means a compressed table, which this reader
    does not support - so an empty schema there is a documented limit, not a failure."""
    opt = gdb_module(spark).findTable(gdb, name, spark.sparkContext._jsc.hadoopConfiguration())
    if not opt.isDefined():
        return False
    path = os.path.join(gdb, opt.get().toTableName() + ".gdbtable")
    if not os.path.exists(path):
        return False
    with open(path, "rb") as fp:
        return struct.unpack("<i", fp.read(4))[0] != 3


def main() -> None:
    gdb = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_GDB
    if not os.path.isdir(gdb):
        sys.exit(f"Not a directory: {gdb}")
    jar = find_jar()
    print(f"jar={os.path.basename(jar)}\ngdb={gdb}\n")

    spark = (
        SparkSession.builder.appName("filegdb-smoke")
        .master("local[*]")
        .config("spark.jars", jar)
        .config("spark.driver.memory", "4g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    failures, skipped = [], []
    try:
        conf = spark.sparkContext._jsc.hadoopConfiguration()
        all_names = list(gdb_module(spark).listTableNames(gdb, conf))
        names = [n for n in all_names if not n.startswith("GDB_")]
        print(f"--- {len(names)} table(s): {names}\n")
        if not names:
            failures.append("catalog listed no user tables")

        for name in names:
            print(f"=== {name}")
            df = spark.read.format("gdb").option("path", gdb).option("name", name).load()
            if not df.schema.fields:
                if is_compressed(spark, gdb, name):
                    print("SKIP: compressed table, unsupported by this reader\n")
                    skipped.append(f"{name}: compressed")
                else:
                    failures.append(f"{name}: empty schema")
                continue
            df.printSchema()

            t0 = time.time()
            n = df.count()
            elapsed = time.time() - t0
            print(f"rows={n} in {elapsed:.2f}s ({n / max(elapsed, 1e-9):,.0f} rows/s)")

            # GDBTableIterator catches any decode error per row and emits every field as null, so a
            # partial decode failure is invisible to a count or a min/max. A row that decoded
            # cleanly always has its non-nullable columns populated - OBJECTID at minimum.
            required = [f.name for f in df.schema.fields if not f.nullable]
            if required and n:
                pred = F.lit(False)
                for c in required:
                    pred = pred | F.col(f"`{c}`").isNull()
                bad = df.filter(pred).count()
                if bad:
                    failures.append(
                        f"{name}: {bad} of {n} row(s) NULL in a non-nullable column {required}"
                        " - rows failed to decode"
                    )

            shape = next(
                (f.name for f in df.schema.fields if "geomType" in f.metadata), None
            )
            shape_type = df.schema[shape].dataType if shape else None
            if shape and not isinstance(shape_type, StructType):
                # Unsupported geometry types fall back to FieldGeomNoop, a StringType column that
                # still carries the geometry metadata. Nothing to validate, and no sub-fields.
                print(f"shape={shape} is {shape_type.simpleString()}, unsupported geometry - not validated")
            elif shape and n == 0:
                print(f"shape={shape} skipped, table is empty")
            elif shape:
                meta = df.schema[shape].metadata
                sub = shape_type.fieldNames()
                # Point shapes carry x/y, everything else carries an xmin..ymax envelope.
                xcol, ycol = ("xmin", "ymin") if "xmin" in sub else ("x", "y")
                xhi, yhi_col = ("xmax", "ymax") if "xmax" in sub else ("x", "y")
                # min/max forces a full decode of every geometry in the table.
                agg = df.selectExpr(
                    f"min(`{shape}`.{xcol}) as xlo", f"max(`{shape}`.{xhi}) as xhi",
                    f"min(`{shape}`.{ycol}) as ylo", f"max(`{shape}`.{yhi_col}) as yhi",
                ).first()
                print(
                    f"shape={shape} meta_extent="
                    f"({meta['xmin']:.4f}, {meta['ymin']:.4f}, {meta['xmax']:.4f}, {meta['ymax']:.4f})"
                    f" data=({agg['xlo']}, {agg['ylo']}, {agg['xhi']}, {agg['yhi']})"
                )
                if agg["xlo"] is None:
                    failures.append(f"{name}: all geometries decoded to null")
                else:
                    # Both axes, both bounds - a swapped or corrupted Y is exactly the kind of
                    # regression the decode rewrites could introduce, and X alone would miss it.
                    for label, value, lo, hi in (
                        ("x", agg["xlo"], meta["xmin"], meta["xmax"]),
                        ("x", agg["xhi"], meta["xmin"], meta["xmax"]),
                        ("y", agg["ylo"], meta["ymin"], meta["ymax"]),
                        ("y", agg["yhi"], meta["ymin"], meta["ymax"]),
                    ):
                        if value is None or not (lo - 1e-6 <= value <= hi + 1e-6):
                            failures.append(
                                f"{name}: decoded {label} {value} outside declared extent [{lo}, {hi}]"
                            )

            df.show(3, truncate=40, vertical=True)
            print()
    finally:
        spark.stop()

    if skipped:
        print("SKIPPED:\n  " + "\n  ".join(skipped))
    if failures:
        print("FAIL:\n  " + "\n  ".join(failures))
        sys.exit(1)
    print("OK")


if __name__ == "__main__":
    main()
