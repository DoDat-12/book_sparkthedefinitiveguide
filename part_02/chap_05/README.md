# Chapter 5. Basic Structured Operations

This chapter focuses exclusively on fundamental DataFrame operations and avoids aggregations, window functions, and
joins

Definitionally, a DataFrame consists of a series of _records_, that are of type `Row`, and a number of _columns_ that
present a computation expression that can be performed on each individual record in the Dataset. _Schemas_ define the
name as well as the type of data in each column. _Partitioning_ of the DataFrame defines the layout of the DataFrame or
Dataset's physical distribution across the cluster. The _partitioning scheme_ defines how that is allocated

Create DataFrame

    // in Scala
    val df = spark.read.format("json")
        .load("/data/flight-data/json/2015-summary.json")

    # in Python
    df = spark.read.format("json").load("/data/flight-data/json/2015-summary.json")

Check schema

    scala> df.printSchema()
    root
    |-- DEST_COUNTRY_NAME: string (nullable = true)
    |-- ORIGIN_COUNTRY_NAME: string (nullable = true)
    |-- count: long (nullable = true)

## Schemas

A schema defines the column names and types of a DataFrame

> For ad hoc analysis, schema-on-read usually works just fine (although at times it can be a bit slow with plain-text
> file formats like CSV or JSON). However, this can also lead to precision issues like a long type incorrectly set as an
> integer when reading in a file. When using Spark for production Extract, Transform, and Load (ETL), it is often a good
> idea to define your schemas manually, especially when working with untyped data sources like CSV and JSON because
> schema inference can vary depending on the type of data that you read in.

Get schema type

    df.schema

- Scala

        org.apache.spark.sql.types.StructType = ...
        StructType(
            StructField(DEST_COUNTRY_NAME,StringType,true),
            StructField(ORIGIN_COUNTRY_NAME,StringType,true),
            StructField(count,LongType,true)
        )

- Python

        StructType(List(StructField(DEST_COUNTRY_NAME,StringType,true),
        StructField(ORIGIN_COUNTRY_NAME,StringType,true),
        StructField(count,LongType,true)))

A schema is a `StructType` made up of a number of fields, `StructFields`, that have a name, type, a Boolean flag which
specifies whether that column can contain missing or `null` values. Users can optionally specify associated metadata
with that column.

Schemas can contain other `StructType`s (Spark's complex types). If the types in the data (at runtime) do not match the
schema, Spark will throw an error.

Example create and enforce a specific schema

![scala specific schema.png](scala%20specific%20schema.png)

In Python

    # in Python
    from pyspark.sql.types import StructField, StructType, StringType, LongType

    myManualSchema = StructType([
      StructField("DEST_COUNTRY_NAME", StringType(), True),
      StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
      StructField("count", LongType(), False, metadata={"hello":"world"})
    ])

    df = spark.read.format("json").schema(myManualSchema)\
      .load("/data/flight-data/json/2015-summary.json")

## Columns and Expressions

You can select, manipulate, and remove columns from DataFrames and these operations are represented as expressions.

### Columns

    import org.apache.spark.sql.functions.{col, column} // Scala
    from pyspark.sql.functions import col, column

    col("someColumnName")
    // or
    column("someColumnName")

Refer to a specific DataFrame's column

    df.col("count")

### Expressions

An `expression` is a set of transformations on one or more values in a record in a DataFrame. Think of it like a
function that takes as input one or more column names, resolves them, and then potentially applies more expressions to
create a single value for each record in the dataset. Importantly, this “single value” can actually be a complex type
like a `Map` or `Array`.

Simplest expression, create via the `expr` function: `expr("someCol")` is equivalent to `col("someCol")`

#### Columns as expressions

    // in Scala
    import org.apache.spark.sql.functions.expr
    expr("(((someCol + 5) * 200) - 6) < otherCol")  // SQL expression

    // Expression as DataFrame code
    (((col("someCol") + 5) * 200) - 6) < col("otherCol")

#### Accessing a DataFrame's columns

Sometimes, you’ll need to see a DataFrame’s columns, which you can do by using something like printSchema; however, if
you want to programmatically access columns, you can use the columns property to see all columns on a DataFrame:

    spark.read.format("json").load("...").columns

## Records and Rows

In Spark, each row in a DataFrame is a single record. Spark represents this record as an object of type Row. Spark
manipulates `Row` objects using column expressions in order to produce usable values

    df.first()

### Creating Rows

It’s important to note that only DataFrames have schemas. Rows themselves do not have schemas. This means that if you
create a Row manually, you must specify the values in the same order as the schema of the DataFrame to which they might
be appended

    // in Scala
    import org.apache.spark.sql.Row
    val myRol = Row("Hello", null, 1, false)

    # in Python
    from pyspark.sql import Row
    myRow = Row("Hello", None, 1, False)

Accessing data in rows

    // in Scala
    myRow(0) // type Any
    myRow(0).asInstanceOf[String] // String
    myRow.getString(0) // String
    myRow.getInt(2) // Int

    # in Python
    myRow[1]
    myRow[2]

## DataFrame Transformations

