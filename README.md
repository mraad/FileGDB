# Spark GDB

In the wake of the unpredictable future of User Defined Types (UDT), this is a hasty minimalist re-implementation of the [spark-gdb](https://github.com/mraad/spark-gdb) project, in such that the content of a [File GeoDatabase](https://www.esri.com/news/arcuser/0309/files/9reasons.pdf) can be mapped to a *read-only* [Spark DataFrame](https://spark.apache.org/docs/latest/sql-programming-guide.html).  It is minimalist as it only supports features with simple geometries (for now :-) with no M or Z.

In the previous implementation, a `GeometryType` was defined using the UDT framework. However, in this implementation, points are stored in a field with two sub-fields x and y. 
~~Polylines and polygons are stored as a string in the [Esri JSON format](https://developers.arcgis.com/documentation/common-data-types/geometry-objects.htm).  It is not the most efficient format, but will make the interoperability with the [ArcGIS API for Python](https://developers.arcgis.com/python/) a bit seamless.~~ Polylines and Polygons shapes are stored as two sub fields, `parts` and `coords`. Parts is an array of integers, where the values are the number of points in the part. Coords is an array of doubles, where the values are a sequence of x,y pairs.

*Notes:*

- This implementation does **not** support compressed file geo databases.
- It is HIGHLY recommended to create a fully compacted feature class before using this implementation.
- The best way to create a compacted feature class is to copy the edited feature class to a new feature class.
- Date field is a timestamp with UTC timezone.

### Changes

This project versions as a single running counter - 0.63, 0.64, 0.65, 0.67 - with no separate
major / minor / patch component. Breaking changes therefore land on an ordinary increment and are
called out explicitly below, as 0.41 was.

- **Version 0.69**

  Reader:
  - New `sortByOffset` option reads each partition in file order instead of OBJECTID order. It
    defaults to off. On edited tables it makes local-disk scans about 5x faster, and S3 scans on
    Spark 3.5 to 4.1 go from more than 5 minutes to about 8 seconds. See
    [Reading in file order](#reading-in-file-order-sortbyoffset) and [Benchmark](#benchmark).

  Build:
  - **Breaking (build only):** the Spark 3.3 and 3.4 profiles are gone. Supported builds are
    Spark 3.5 (the default, Scala 2.12, Java 8 bytecode) and Spark 4.0, 4.1 and 4.2
    (`-Pspark-4.0`, `-Pspark-4.1`, `-Pspark-4.2`; Scala 2.13, Java 17).

  Smoke test:
  - Runs the JVM of the pyspark it imports. An inherited `SPARK_HOME` used to start that
    install's Spark instead, so the run checked a different Spark than the one it reported.
  - Loads the jar built for the running pyspark's Spark line rather than the newest jar in
    `target/`.
  - Reads `s3a://` paths. `ozone/docker-compose.yaml` runs a local S3 endpoint to test against.
  - Binds the driver to loopback, because a host firewall can reset the driver's connection to
    its own LAN address and abort startup.

- **Version 0.68**: the default build moves to Spark 3.5.9 and Scala 2.12.21. A Spark 4.0 /
  Scala 2.13 profile was added later without a version bump.

- **Version 0.67** (supersedes 0.66, which was never released).

  **Breaking:** `FieldBinary.readValue` returns `Array[Byte]` where it used to return a
  `ByteBuffer`. Reading a *non-null* `BinaryType` column through the Spark data source previously
  failed outright - the `ByteBuffer` survives `CatalystTypeConverters` but dies at
  `UnsafeProjection` with
  `ClassCastException: class java.nio.HeapByteBuffer cannot be cast to class [B` - so no working
  Spark code can depend on the old type. That `ByteBuffer` was also a view over a buffer reused
  between rows, so its contents changed underneath the caller. Code using the non-Spark
  `FileGDB.rows` / `FileGDB.apply` APIs on a binary column must cast to `Array[Byte]`.

  Correctness:
  - `FileGDB.rows(path, name, conf)` used to stop after `numFeatures` **index slots** rather than
    scanning them all. `numFeatures` counts live rows, but deleted rows still occupy slots, so on
    an edited (uncompacted) table the call silently returned a fraction of the features -
    `Miami.gdb` `Broadcast` returned 162,970 of 1,365,578. The Spark data source was never
    affected; `GDBRDD` always passes an explicit slot count.
  - `GDBIndex.indices` now clamps `startRow` and `numRows` to what the `.gdbtablx` actually holds.
    An over-requested page previously ran off the end of the file and threw `EOFException` instead
    of returning the rows that were there. `FileGDB#rows(numRowsToRead, startAtRow)` is a paging
    API with no clamping of its own. This bug predates 0.66.
  - `GDBTable.rows` returns an empty iterator when the header could not be parsed, rather than
    scanning index slots of a file it already knows it cannot read.
  - Header caches were plain `mutable.Map` mutated by concurrent tasks sharing an executor JVM,
    racing in `getOrElseUpdate`. Now `TrieMap`, keyed on path + length + modification time so
    regenerating a `.gdb` at the same path no longer decodes new records at stale field offsets,
    and capped so entries cannot accumulate without bound.
  - The index block buffer is positioned per row and sized by a byte budget, so a `numBytesPerRow`
    outside 4..6 can no longer desynchronize the reader and a garbage value can no longer be
    amplified into an oversized allocation. Values outside 4..6 are rejected outright; 7 and 8 byte
    rows belong to the V4 / 64-bit OBJECTID format, which this reader does not support.
  - `startRow * numBytesPerRow` was `Int` arithmetic, overflowing past ~536M rows.

  Performance - roughly 1.4x on attribute-heavy tables, 1.1x where geometry decoding dominates:
  - `getInt`/`getLong` do one `readInt`/`readLong` plus `reverseBytes` rather than 4/8 `readByte`
    calls through the `FSDataInputStream` stack.
  - `.gdbtablx` is read in blocks instead of one `readFully` per 4-6 byte row.
  - `FieldBytes.fillVarBytes` bulk copies; this hits every string and every geometry.
  - `GDBTableIterator.next` fills a preallocated array in a `while` loop; the old `fields.map`
    closed over a `var`, boxing the bit counter on every row.
  - `FieldUUID` builds its hex by hand; `String.format` re-parsed a 16-arg pattern per row.
  - `FieldBinary` copies each blob once instead of twice.

  Build:
  - The scalatest JVM died at startup on JDK 17 with an `IllegalAccessError` on
    `sun.nio.ch.DirectBuffer`, so no test had been running. The `--add-opens` flags now come from a
    `jdk9+` profile, which required moving the default build properties out of the
    `activeByDefault` spark-3.5 profile: Maven deactivates `activeByDefault` profiles as soon as
    any other profile auto-activates.
- Sep 10, 2021, Version 0.41 is a breaking change in the `FileGDB` object.

## Building the project using [Maven](https://maven.apache.org/):

```bash
mvn clean install                # Spark 3.5, Scala 2.12 - target/filegdb-0.69-3.5-2.12.jar
mvn clean install -Pspark-4.0    # Spark 4.0, Scala 2.13 - target/filegdb-0.69-4.0-2.13.jar
mvn clean install -Pspark-4.1    # Spark 4.1, Scala 2.13
mvn clean install -Pspark-4.2    # Spark 4.2, Scala 2.13
```

The Spark 4 builds need JDK 17 or later.

## Usage

The best demonstration of the usage of this implementation is with [PySpark DataFrames](https://docs.databricks.com/spark/latest/dataframes-datasets/introduction-to-dataframes-python.html) and in conjunction with the [ArcGIS API for Python](https://developers.arcgis.com/python/).

Create the local Python environment with [uv](https://docs.astral.sh/uv/) and smoke test the freshly
built jar against a real File GeoDatabase:

```bash
uv sync
uv run smoke_test.py /path/to/some.gdb                         # Spark 3.5 (pinned in pyproject.toml)
uv run --with pyspark==4.2.0 smoke_test.py /path/to/some.gdb   # a Spark 4 line, after -Pspark-4.2
```

`smoke_test.py` lists the feature classes, prints each schema, counts the rows, and flags rows that
failed to decode by looking for NULLs in non-nullable columns. For a supported geometry type in a
non-empty table it also forces a full geometry decode and checks the result falls inside the extent
declared in the field metadata, on both axes. Empty tables and geometry types this reader does not
support are reported and left unvalidated; compressed tables are reported as skipped. With no
argument it runs against the `data/Miami.gdb` fixture in this repository.

To test reading from S3, start the single-node [Apache Ozone](https://ozone.apache.org/) S3 gateway
in `ozone/` (with Docker, or with [Colima](https://github.com/abiosoft/colima) on macOS). Ozone
accepts any credentials because the cluster is unsecured. Copy a `.gdb` into a bucket, then pass
the `s3a://` path to the smoke test. The smoke test adds the `hadoop-aws` version that matches the
Hadoop inside pyspark.

```bash
docker compose -f ozone/docker-compose.yaml up -d
export AWS_ACCESS_KEY_ID=ozone AWS_SECRET_ACCESS_KEY=ozone AWS_ENDPOINT_URL=http://localhost:9878
aws s3 mb s3://filegdb
aws s3 sync data/Miami.gdb s3://filegdb/Miami.gdb
uv run smoke_test.py s3a://filegdb/Miami.gdb
```

Assuming that the environment variable `SPARK_HOME` points to the location of a Spark installation, start a Jupyter notebook that is backed by PySpark:

```bash
export PATH=${SPARK_HOME}/bin:${PATH}
export SPARK_LOCAL_IP=$(hostname)
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
pyspark\
 --master local[*]\
 --num-executors 1\
 --driver-memory 16G\
 --executor-memory 16G\
 --jars target/filegdb-0.69-3.5-2.12.jar
```

That `--jars` path is the default build (Spark 3.5, Scala 2.12). The jar name encodes
`${filegdb.version}-${spark.compact}-${scala.compact}`, so substitute the matching filename if you
built another profile - `-Pspark-4.2` produces `filegdb-0.69-4.2-2.13.jar`. This POM has no
`distributionManagement`, so `mvn install` publishes to your local repository only - there is no
Central coordinate to pass to `--packages`.

Here is yet another example in Scala:

```scala
import com.esri.gdb._

val path = "data/Miami.gdb"
val name = "Broadcast"

val spark = SparkSession.builder().getOrCreate()
try
{
    spark
      .read
      .gdb(path, name)
      .createTempView(name)

    spark
      .sql(s"select MMSI, count(*) as reports, avg(Shape.x) as x, avg(Shape.y) as y from $name group by MMSI order by reports desc limit 10")
      .collect()
      .foreach(println)
}
finally
{
    spark.stop()
}
```

### Options

| Option | Default | Meaning |
|---|---|---|
| `path` | required | Path to the `.gdb` folder: local, `hdfs://`, `s3a://` or any other Hadoop filesystem. |
| `name` | `GDB_SystemCatalog` | Table or feature class to read. |
| `numPartitions` | `spark.default.parallelism`, else 8 | Number of slices the table is split into. A table with 1,024 or fewer index slots is always read as one slice. |
| `sortByOffset` | `false` | Read each partition's rows in the order they are stored in the `.gdbtable` file instead of OBJECTID order. See [below](#reading-in-file-order-sortbyoffset). |

#### Reading in file order (`sortByOffset`)

By default a partition reads its rows in OBJECTID order, following the `.gdbtablx` index. In a
table that was loaded once and never edited, that is also the order the rows are stored in the
`.gdbtable` file. Edits and deletes break that. A rewritten row moves to a new place in the file,
so reading in OBJECTID order turns into random access. In `data/Miami.gdb`, `Broadcast` is such a
table: about half of all consecutive rows jump backward in the file, and half jump more than 64 KB
forward.

With `sortByOffset` turned on, each partition sorts its index entries by file offset before it
reads, so the scan becomes one forward pass through its part of the file. The same rows come back
either way. The test suite checks this with a row count and a hash of every column.

PySpark:

```python
df = (spark.read.format("gdb")
      .option("path", "data/Miami.gdb")
      .option("name", "Broadcast")
      .option("sortByOffset", "true")
      .load())
```

Scala:

```scala
import com.esri.gdb._

val df = spark.read.gdb("data/Miami.gdb", "Broadcast", sortByOffset = true)
```

SQL:

```sql
CREATE TEMPORARY VIEW broadcast
USING gdb
OPTIONS (path 'data/Miami.gdb', name 'Broadcast', sortByOffset 'true')
```

Turn it on when:
- the table has been edited, and
- you read from an object store with Spark 3.5 to 4.1, or you want a faster scan on any filesystem.

Leave it off when:
- downstream code relies on rows coming out of a partition in OBJECTID order, or
- one partition would hold a very large number of rows. Each partition keeps its index entries in
  memory while sorting, at about 32 bytes per row, so raise `numPartitions` for very large tables.

### Benchmark

These are full scans, decoding every row of every table, with and without `sortByOffset`. Each GDB
is read from local disk and from S3 (Apache Ozone). Each cell is in seconds and is the best of two
runs. A run that passed 300 s was cancelled and is shown as "> 300".

| GDB | Source | Row order | Spark 3.5.9<br>Hadoop 3.3.4 | Spark 4.0.4<br>Hadoop 3.4.1 | Spark 4.1.3<br>Hadoop 3.4.2 | Spark 4.2.0<br>Hadoop 3.5.0 |
|---|---|---|---:|---:|---:|---:|
| Miami | local disk | OBJECTID (default) | 2.2 | 2.1 | 2.1 | 2.1 |
| Miami | local disk | `sortByOffset` | 0.3 | 0.4 | 0.4 | 0.4 |
| Miami | S3 | OBJECTID (default) | > 300 | > 300 | > 300 | 2.9 |
| Miami | S3 | `sortByOffset` | 8.5 | 8.2 | 8.3 | 3.0 |
| NorthSea | local disk | OBJECTID (default) | 0.1 | 0.1 | 0.1 | 0.1 |
| NorthSea | local disk | `sortByOffset` | 0.1 | 0.1 | 0.1 | 0.1 |
| NorthSea | S3 | OBJECTID (default) | 1.0 | 0.9 | 0.9 | 0.2 |
| NorthSea | S3 | `sortByOffset` | 0.3 | 0.3 | 0.3 | 0.2 |
| World | local disk, S3 | either | n/a | n/a | n/a | n/a |

- **Miami** (`data/Miami.gdb`, 186 MB) is dominated by `Broadcast`. That table has 1,365,578 live
  rows spread over 11.8M index slots, and its rows are stored in near-random order. Unsorted, about
  half of consecutive reads jump backward in the file and half jump more than 64 KB forward.
- **NorthSea** (8.4 MB, three small feature classes) is close to file order already, so sorting
  only matters over S3.
- **World** (`data/World.gdb`) has no time because its only table, `Countries`, is CDF-compressed,
  which this reader does not support. `bench.py` skips it on both sources.
- **Why Spark 3.5 to 4.1 stall on S3:** their S3A client (Hadoop 3.3.4 to 3.4.2) reopens the HTTP
  stream for every seek outside its current 64 KB read-ahead window. An unsorted scan of a
  scattered table therefore sends about one ranged GET per row. Sorting brought `Broadcast` down to
  989 stream opens. The Hadoop 3.5.0 S3A client in Spark 4.2 handles the same access pattern
  with a few dozen stream opens, so on 4.2 the option changes little over S3.

Setup: Apple M4 Max with 16 cores and 128 GB, macOS 26, Spark `local[*]` with 8 partitions per
table. Ozone 2.2.1 ran from `ozone/docker-compose.yaml` in a Colima VM (8 CPUs, 16 GB) on the same
machine, so the S3 numbers have no real network latency. A remote object store would widen the
gap. Local reads ran with a warm OS page cache. Miami was on an external drive and NorthSea on the
internal SSD.

To reproduce, build the matching profile first, then run:

```bash
export AWS_ACCESS_KEY_ID=ozone AWS_SECRET_ACCESS_KEY=ozone AWS_ENDPOINT_URL=http://localhost:9878
uv run --with pyspark==4.2.0 bench.py data/Miami.gdb s3a://filegdb/Miami.gdb   # BENCH_CAP=300 by default
```

## TODO

- **Write test cases. Come on Mansour, u know better !!**
- ~~Save geometry as a struct(type,xmin,ymin,xmax,ymax,parts,coords)~~
- Add option to skip reading the geometry.
- Add option to return geometry envelope only.
- Add option to return timestamp field as millis long.
- Read geometry as WKB.
- ~~Add geometry extent as subfields to `Shape`.~~

### Notes To Self

- Install JDK-1.8
- Set path to %JAVA_HOME%\bin,%JAVA_HOME%\jre\bin
- keytool -import -alias cacerts -keystore cacerts -file C:\Windows\System32\documentdbemulatorcert.cer

### References

- https://github.com/rouault/dump_gdbtable/wiki/FGDB-Spec
- https://github.com/minrk/findspark
- https://blog.sicara.com/get-started-pyspark-jupyter-guide-tutorial-ae2fe84f594f
- https://github.com/maxpoint/spylon
- https://github.com/jupyter-scala/jupyter-scala#quick-start
- https://github.com/Valassis-Digital-Media/spylon-kernel/blob/master/examples/basic_example.ipynb
- https://ncar.github.io/PySpark4Climate/tutorials/Oceanic-Ni%C3%B1o-Index/
- https://medium.com/@marcovillarreal_40011/creating-a-spark-standalone-cluster-with-docker-and-docker-compose-ba9d743a157f
