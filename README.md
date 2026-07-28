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
mvn clean install
```

## Usage

The best demonstration of the usage of this implementation is with [PySpark DataFrames](https://docs.databricks.com/spark/latest/dataframes-datasets/introduction-to-dataframes-python.html) and in conjunction with the [ArcGIS API for Python](https://developers.arcgis.com/python/).

Create the local Python environment with [uv](https://docs.astral.sh/uv/) and smoke test the freshly
built jar against a real File GeoDatabase:

```bash
uv sync
uv run smoke_test.py /path/to/some.gdb
```

`smoke_test.py` lists the feature classes, prints each schema, counts the rows, and flags rows that
failed to decode by looking for NULLs in non-nullable columns. For a supported geometry type in a
non-empty table it also forces a full geometry decode and checks the result falls inside the extent
declared in the field metadata, on both axes. Empty tables and geometry types this reader does not
support are reported and left unvalidated; compressed tables are reported as skipped. With no
argument it runs against the `data/Miami.gdb` fixture in this repository.

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
 --jars target/filegdb-0.67-3.5-2.12.jar
```

That `--jars` path is the default build (Spark 3.5, Scala 2.12). The jar name encodes
`${filegdb.version}-${spark.compact}-${scala.compact}`, so substitute the matching filename if you
built another profile - `-Pspark-3.4` produces `filegdb-0.67-3.4-2.12.jar`. This POM has no
`distributionManagement`, so `mvn install` publishes to your local repository only - there is no
Central coordinate to pass to `--packages`.

Check out the [Broadcast](Broadcast.ipynb) and [Countries](Countries.ipynb) example notebooks.

Here is yet another example in Scala:

```scala
import com.esri.gdb._

val path = "World.gdb"
val name = "Countries"

val spark = SparkSession.builder().getOrCreate()
try
{
    spark
      .read
      .gdb(path, name)
      .createTempView(name)

    spark
      .sql(s"select CNTRY_NAME,SQKM from $name where SQKM < 10000.0 ORDER BY SQKM DESC LIMIT 10")
      .collect()
      .foreach(println)
}
finally
{
    spark.stop()
}
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
