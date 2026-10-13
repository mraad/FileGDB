"""Time a full decode of every table in each GDB, in OBJECTID order and with sortByOffset.

Usage: uv run [--with pyspark==4.2.0] bench.py GDB [GDB ...]   (local paths and/or s3a://)

Prints one markdown row per GDB: | gdb | default order (s) | sortByOffset (s) |. Each cell is the
best of two runs; a run longer than BENCH_CAP seconds (default 300) is cancelled and shown as
"> cap". s3a:// needs the same environment as smoke_test.py.
"""

import os
import sys
import threading
import time

from smoke_test import gdb_module, session

CAP = float(os.environ.get("BENCH_CAP", "300"))


def scan(spark, gdb: str, names: list, sort: bool) -> float:
    """Seconds to decode every row of every table. noop forces the full decode a count might skip."""
    t0 = time.time()
    for name in names:
        (spark.read.format("gdb").option("path", gdb).option("name", name)
         .option("sortByOffset", str(sort).lower()).load()
         .write.format("noop").mode("overwrite").save())
    return time.time() - t0


def capped(spark, gdb: str, names: list, sort: bool):
    """scan(), cancelled after CAP seconds - None if it was."""
    sc = spark.sparkContext
    sc.setJobGroup("bench", "bench", interruptOnCancel=True)
    timer = threading.Timer(CAP, sc.cancelJobGroup, ["bench"])
    timer.start()
    try:
        return scan(spark, gdb, names, sort)
    except Exception:
        if timer.finished.is_set():
            return None
        raise
    finally:
        timer.cancel()


def best(spark, gdb: str, names: list, sort: bool) -> str:
    # The first run also pays JIT warm-up and the S3 connection setup; a capped one is not retried.
    runs = []
    for _ in range(2):
        secs = capped(spark, gdb, names, sort)
        if secs is None:
            return f"> {CAP:.0f}"
        runs.append(secs)
    return f"{min(runs):.1f}"


def main() -> None:
    gdbs = [g.rstrip("/") for g in sys.argv[1:]]
    if not gdbs:
        sys.exit(__doc__)
    spark = session("filegdb-bench", any(g.startswith("s3a://") for g in gdbs))
    try:
        conf = spark.sparkContext._jsc.hadoopConfiguration()
        print("| gdb | default order (s) | sortByOffset (s) |\n|---|---|---|")
        for gdb in gdbs:
            # A compressed table infers an empty schema and has no .gdbtablx to scan.
            names = [
                n for n in gdb_module(spark).listTableNames(gdb, conf)
                if not n.startswith("GDB_")
                and spark.read.format("gdb").option("path", gdb).option("name", n).load().schema.fields
            ]
            if not names:
                print(f"| {gdb} | no readable table (compressed) | |", flush=True)
                continue
            print(f"| {gdb} | {best(spark, gdb, names, False)} | {best(spark, gdb, names, True)} |", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
