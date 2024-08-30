# Chapter 9. Data Sources

## The Structure of the Data Sources API

### Read API Structure

The core structure for reading data

    DataFrameReader.format(...).option("key", "value").schema(...).load()

- `format` is optional because by default Spark will use the Parquet format
- `option` allows you to set key-value configuration to parameterize how you will read data
- `schema` is optional if the date source provides a schema or if you intend to use schema inference

### Basics of Reading Data

`DataFrameReader` can be accessed through the `SparkSession` via the `read` attribute

    spark.read

After we have a DataFrame reader, we specify several values: The _format_, _schema_, _read mode_, series of _option_

    spark.read.format("csv")
      .option("mode", "FAILFAST")
      .option("inferSchema", "true")
      .option("path", "path/to/file(s)")
      .schema(someSchema)
      .load()

**Read modes (option "mode")**

- `permissive`: (default) Set all fields to `null` when it encounters a corrupted record and places all corrupted
  records in a string called `_corrupt_record`
- `dropMalformed`: Drops the row that contains malformed records
- `failFast`: Fails immediately upon encountering malformed records

> _malformed_ - Không đúng định dạng

### Write API Structure

The core structure of writing data

    DataFrameWriter.format(...).option(...).partitionBy(...).bucketBy(...).sortBy(...).save()

- `format` is optional because Spark will use the parquet format by default
- `option` configures how to write our given data
- `partitionBy` and `sortBy` work only for file-based data sources

### Basics of Writing Data

Access the DataFrameWriter on a per-`DataFrame` basis via the `write` attribute

    dataFrame.write

    dataFrame.write.format("csv")
      .option("mode", "OVERWRITE")
      .option("dateFormat", "yyyy-MM-dd")
      .option("path", "path/to/file(s)")
      .save()

**Save modes**

- `append`: Appends the output files to the list of files that already exist at that location
- `overwrite`: Completely overwrite any data that already exists there
- `errorIfExists` : Throws an error and fails the write if data or files already exist at the specified location
- `ignore`: If data or files exist at the location, do nothing with the current DataFrame

## CSV Files

CSV stands for comma-separated values. This is a common text file format in which each line represents a single record,
and commas separate each field within a record. CSV files, while seeming well-structured, are actually one of the
trickiest file formats you will encounter because not many assumptions can be made in production scenarios about what
they contain or how they are structured

CSV Options: https://spark.apache.org/docs/latest/sql-data-sources-csv.html

### Reading CSV Files

First create a `DataFrameReader`

    spark.read.format("csv")

Using option to specify ...

    spark.read.format("csv")
      .option("header", "true")
      .option("mode", "FAILFAST")
      .option("inferSchema", "true")
      .load("some/path/to/file.csv")

Using schema we create

    val myManualSchema = new StructType(Array(
      new StructField("DEST_COUNTRY_NAME", StringType, true),
      new StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      new StructField("count", LongType, false)
    ))

    spark.read.format("csv")
      .option("header", "true")
      .option("mode", "FAILFAST")
      .schema(myManualSchema)
      .load("D:\\repo_books\\book_sparkthedefinitiveguide\\data\\flight-data\\csv\\2010-summary.csv")
      .show(5, truncate = false)

### Writing CSV Files

    csvDF.write.format("csv")
      .mode("overwrite")
      .option("sep", "\t")
      .save("D:\\repo_books\\spark_chap09\\output\\my-tsv-files")  // folder

`my-tsv-files` is a folder with numerous files within it. This actually reflects the number of partitions in our
DataFrame at the time we write it out. If we were to repartition our data before then, we would end up with a different
number of files

## JSON Files

In Spark, when we refer to JSON files, we refer to _line-delimited_ JSON files

JSON data source options: https://spark.apache.org/docs/latest/sql-data-sources-json.html

Line-delimited JSON is actually a much more stable format because it allows you to append to a file with a new record (
rather than having to read in an entire file and then write it out), which is what we recommend that you use

    spark.read.format("json")

### Reading JSON Files

    spark.read.format("json")
      .option("mode", "FAILFAST")
      .schema(myManualSchema)
      .load("D:\\repo_books\\book_sparkthedefinitiveguide\\data\\flight-data\\json\\2010-summary.json")
      .show(5)

### Writing JSON Files

Writing JSON files is just as simple as reading them, and, as you might expect, the data source does not matter.
Therefore, we can reuse the CSV DataFrame that we created earlier to be the source for our JSON file

