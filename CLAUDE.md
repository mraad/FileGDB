# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A read-only Spark data source (`format("gdb")`) for Esri File GeoDatabase tables, written in Scala. It parses
`.gdbtable` / `.gdbtablx` files directly through the Hadoop `FileSystem` API, so it works on local paths and
HDFS/S3 alike. It does not support compressed tables. On-disk format reference:
https://github.com/rouault/dump_gdbtable/wiki/FGDB-Spec

## Build and test (Maven; the `project/*.sbt` files are a dead sbt-0.13 leftover)

```bash
mvn clean install -DskipTests                 # default: Spark 3.5.x / Scala 2.12 / Java 8 target
mvn clean install -DskipTests -Pspark-4.2     # Spark 4.2 / Scala 2.13 / Java 17; also -Pspark-4.0, -Pspark-4.1
mvn test                                      # all suites (scalatest-maven-plugin; surefire is disabled)
mvn test -Dsuites=com.esri.gdb.GDBSuite                  # one suite
mvn test -Dsuites='com.esri.gdb.GDBSuite Read Miami'     # tests in a suite whose name contains the substring
```

- The jar is `target/filegdb-${filegdb.version}-${spark.compact}-${scala.compact}.jar` (for example
  `filegdb-0.69-3.5-2.12.jar`). The version is `filegdb.version` in `pom.xml`.
- Spark 3.5 is **not** a profile. Its settings are the top-level `<properties>`, because Maven turns off
  `activeByDefault` profiles once another profile auto-activates, and the `jdk9+` profile always does on modern
  JDKs. The `jdk9+` profile supplies the `--add-opens` test JVM flags. Without them the test JVM dies at startup.
  The gitignored `mvn35.sh` / `mvn4{0,1,2}.sh` helpers run `clean install -DskipTests` for each line.
- `clean` wipes `target/`, so only one profile's jar exists there at a time. Build the profile before you smoke
  test it.
- Main code must compile on Scala 2.12 **and** 2.13 and on a Java 8 target. For example, the code widens
  `ByteBuffer` to `Buffer` before calling `position(int)`, because that override is covariant only from Java 9.
- Test fixtures: `GDBSuite` reads paths relative to the repo root. `data/Miami.gdb` is the readable fixture that
  every clone has. `data/World.gdb` is tracked too, but its only table is CDF-compressed and therefore unreadable.
  The suite's other fixtures are gitignored or untracked, so on a fresh clone the tests that use them fail.
  `ReadSuite` is `@Ignore`d.

PySpark scripts, run with `uv run` (README "Usage" and "Benchmark" have the details):
- `smoke_test.py [gdb]` checks that every table decodes.
- `bench.py gdb...` times full scans with and without `sortByOffset`.
- Add `--with pyspark==4.x.y` to run on a Spark 4 line.

Both scripts share `smoke_test.session()`:
- It points `SPARK_HOME` at the imported pyspark, because the user's shell exports an sdkman Spark.
- It loads `target/filegdb-*-<pyspark major.minor>-*.jar`, so build the matching profile first.
- It adds S3A support when a path is `s3a://`.

For S3, start Colima with `colima start pgx` and use `DOCKER_CONTEXT=colima-pgx`, because the default docker
context is OrbStack. Then follow the README's Ozone steps.

## Architecture (all in `src/main/scala/com/esri/gdb`)

**Entry points.** `DefaultSource` is registered via `META-INF/services` as short name `gdb` and takes the
options `path`, `name`, `numPartitions` and `sortByOffset`. `name` defaults to `GDB_SystemCatalog`. `package.scala` adds
`.gdb(path, name)` implicits on `DataFrameReader`, `SQLContext` and `SparkContext`. `FileGDB` (object and
class) is a non-Spark API for listing tables, reading schemas and iterating rows.

**Table lookup.** `FileGDB.listTables` reads the system catalog `a00000001.gdbtable` to map a user table name to
its numeric ID. `NameIndex.toTableName` formats that ID as the on-disk base name `a%08x`.

**Read path.** `GDBRelation` (a V1 `TableScan`) infers the schema from the table header. `buildScan` returns a
`GDBRDD`. `GDBRDD.getPartitions` splits the `.gdbtablx` **index slots** (`GDBIndex.maxRows`), not live rows,
into `numPartitions` ranges. Deleted rows still take up slots and have seek offset 0, and `GDBIndexIterator`
skips them. `compute` opens a `GDBIndex` and a `GDBTable` per partition and registers them as
`TaskCompletionListener`s so they get closed.

**Row order.** By default each partition reads rows in OBJECTID order, which means random seeks on an edited
table. `sortByOffset` sorts the partition's index rows by file offset in `GDBTable.rows` (in place, using
`GDBIndexRow.BySeek`). That holds the partition's index rows in memory. The README "Benchmark" section has the
numbers.

**Decoding.** `GDBTable.apply` parses the header into `GDBField` decoders, one per column. `GDBTableIterator`
seeks to each row's offset, reads the null bitmap, and calls `readValue` or `readNull` on each field. A row that
fails to decode is logged and returned as all nulls; it does not fail the task. `smoke_test.py` relies on this to
find decode failures.

**Fields.** Scalar types are in `GDBField.scala`. Geometry decoders are chosen by geometry type × the Z/M bits
of `geometryProp`: `FieldXY{,M,Z,ZM}` for points, `FieldMultiPoint*` for multipoints, and `FieldMultiPart*`
for polylines and polygons. Points become a struct `(x, y[, z][, m])`. Multipart shapes become a struct with
`parts` (point counts) and `coords` (flat x,y arrays). Unsupported types (for example multipatch) fall back to
`FieldGeomNoop`. Geometry field metadata holds the extent, `srsWKT`, `hasZ` and `hasM`. Coordinates are
varint-encoded deltas, scaled by `OrigScale`.

**Invariants to preserve:**
- Header caches (`GDBTable.map`, and the one in `GDBIndex`) are `TrieMap`s shared by concurrent tasks in an
  executor JVM. Their keys are `cacheKey` (path + length + mtime), and `cachedHeader` caps their size.
- `DataBuffer` and its `ByteBuffer` are **reused across rows**. Field decoders must copy anything they return
  (for example, `FieldBinary` returns a fresh `Array[Byte]`). `GDBTable.rows` gives each iterator its own
  decoders through `GDBField.copy()`, because some decoders keep per-row state.
- `.gdbtablx` row widths are only 4–6 bytes. 7- and 8-byte widths (the V4 / 64-bit OBJECTID format) are rejected,
  not supported. `GDBIndex.indices` clamps `startRow` and `numRows` to the file size.
- The hot path is written with `while` loops and no closures on purpose, for performance. Keep it that way.
- New classes that get shipped to executors should be added to `GDBRegistrator` (Kryo).

## Repo conventions

- Versioning is a single counter (0.67, 0.68, 0.69, …). Breaking changes are called out in the README "Changes"
  section, not with a major-version bump. When the version changes, update `pom.xml`, `pyproject.toml` and the
  README jar names together.
- Deliberate shortcuts are marked with `ponytail:` / `shortcut:` comments that name the limit.
- `.coderabbit.yaml` configures CodeRabbit PR review. Titles containing `[WIP]`, `WIP:` or `DO NOT MERGE` are
  not auto-reviewed.
