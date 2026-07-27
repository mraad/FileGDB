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

- Version 0.67:
  - `GDBIndex.indices` now clamps `startRow` and `numRows` to what the `.gdbtablx` actually holds.
    An over-requested page previously ran off the end of the file and threw `EOFException` instead
    of returning the rows that were there - `FileGDB#rows(numRowsToRead, startAtRow)` is a paging
    API with no clamping of its own. This bug predates 0.66.
  - `GDBTable.rows` returns an empty iterator when the header could not be parsed, rather than
    scanning index slots of a file it already knows it cannot read.
  - The index block buffer is positioned per row and sized by a byte budget, so a `numBytesPerRow`
    outside 4..6 can no longer desynchronize the reader, and a garbage value can no longer be
    amplified into an oversized allocation. Values outside 4..8 are now rejected outright.
  - Header caches are keyed on path + length + modification time and capped, so regenerating a
    `.gdb` at the same path no longer decodes new records with stale field offsets for the life
    of the JVM.
  - `FieldBinary` copies each blob once instead of twice.
  - `--add-opens` moved into a `jdk9+` profile. The default build properties moved out of the
    `activeByDefault` spark-3.5 profile to the top-level `<properties>` block, because Maven
    deactivates `activeByDefault` profiles as soon as any other profile auto-activates.
- Version 0.66:
  - `FileGDB.rows(path, name, conf)` used to stop after `numFeatures` **index slots** rather than
    scanning them all. On an edited (uncompacted) table, deleted rows occupy slots, so the call
    silently returned only a fraction of the features. The Spark data source was never affected.
  - `FieldBinary` returned a `ByteBuffer` view over a reused buffer for a `BinaryType` column -
    now an `Array[Byte]` copy, which is what Spark actually expects. **Breaking** for code reading
    a `BinaryType` column through the non-Spark `FileGDB.rows` / `FileGDB.apply` APIs, which hand
    back the decoded value untouched: cast to `Array[Byte]`, not `ByteBuffer`.
  - The `.gdbtable` / `.gdbtablx` header caches are now concurrent; several tasks share one executor JVM.
  - `.gdbtablx` is read in 4096-row blocks, `getInt`/`getLong` in one call, and field bytes are bulk
    copied. Roughly 1.4x on attribute-heavy tables, 1.1x when geometry decoding dominates.
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

`smoke_test.py` lists the feature classes, prints each schema, counts the rows, and checks that the
decoded geometry falls inside the extent declared in the field metadata.

Assuming that the environment variable `SPARK_HOME` points to the location of a Spark installation, start a Jupyter notebook that is backed by PySpark:

```bash
export PATH=${SPARK_HOME}/bin:${PATH}
export SPARK_LOCAL_IP=$(hostname)
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
export GDB_MIN=2.11 # Spark 2.3
# export GDB_MIN=2.12 # Spark 2.4
export GDB_VER=0.18
pyspark\
 --master local[*]\
 --num-executors 1\
 --driver-memory 16G\
 --executor-memory 16G\
 --packages com.esri:webmercator_${GDB_MIN}:1.4,com.esri:filegdb_${GDB_MIN}:${GDB_VER}
```

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
