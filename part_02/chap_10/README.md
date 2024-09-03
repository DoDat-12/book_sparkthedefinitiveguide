# Chapter 10. Spark SQL

In a nutshell, with Spark SQL you can run SQL queries against views or tables organized into databases. You also can use
system functions or define user functions and analyze query plans in order to optimize their workloads

## What is SQL?

SQL or _Structured Query Language_ is a domain-specific language for expressing relational operations over data. It is
used in all relational databases, and many “NoSQL” databases create their SQL dialect in order to make working with
their databases easier

## Big Data and SQL: Apache Hive

Before Spark’s rise, Hive was the de facto big data SQL access layer. In many ways it helped propel Hadoop into
different industries because analysts could run SQL queries. Although Spark began as a general processing engine with
Resilient Distributed Datasets (RDDs), a large cohort of users now use Spark SQL

## Big Data and SQL: Spark SQL

The power of Spark SQL derives from several key facts: SQL analysts can now take advantage of Spark’s computation
abilities by plugging into the Thrift Server or Spark’s SQL interface, whereas data engineers and scientists can use
Spark SQL where appropriate in any data flow

### Spark's Relationship to Hive

Spark's SQL has a great relationship with Hive because it can connect to Hive metastores. The Hive metastore is the way
in which Hive maintains table information for use across sessions. With Spark SQL, you can connect to your Hive
metastore (if you already have one) and access table metadata to reduce file listing when accessing information

**The Hive metastore**

If you're connecting to Hive
metastore: https://spark.apache.org/docs/latest/sql-programming-guide.html#interacting-with-different-versions-of-hive-metastore

## How to Run Spark SQL Queries

### Spark SQL CLI

![spark sql cli.png](spark%20sql%20cli.png)

You configure Hive by placing your _hive-site.xml_, _core-site.xml_, and _hdfs-site.xml_ files in _conf/_

> For complete list of all available options: `spark-sql --help`

### Spark's Programmatic SQL Interface

In addition to setting up a server, you can also execute SQL in an ad hoc manner via any of Spark’s language APIs

    spark.sql("SELECT 1 + 1").show()

The command spark.sql("SELECT 1 + 1") returns a DataFrame that we can then evaluate programmatically. Just like other
transformations, this will not be executed eagerly but lazily. This is an immensely powerful interface because there are
some transformations that are much simpler to express in SQL code than in DataFrames.

You can completely interoperate between SQL and DataFrames

    spark.read.format("json")
      .load("D:\\repo_books\\book_sparkthedefinitiveguide\\data\\flight-data\\json\\2015-summary.json")
      .createOrReplaceTempView("some_sql_view") // DF to SQL

    spark.sql(
        """
      SELECT DEST_COUNTRY_NAME, SUM(count)
      FROM some_sql_view
      GROUP BY DEST_COUNTRY_NAME
      """)
      .where("DEST_COUNTRY_NAME LIKE 'S%'").where("`SUM(count)` > 10")
      .show()

### SparkSQL Thrift JDBC/ODBC Server

Spark provides a Java Database Connectivity (JDBC) interface by which either you or a remote program connects to the
Spark driver in order to execute Spark SQL queries. A common use case might be for a business analyst to connect
business intelligence software like Tableau to Spark

The Thrift JDBC/Open Database Connectivity (ODBC) server implemented here corresponds to the HiveServer2 in Hive 1.2.1

Start the JDBC/ODBC server

    C:\spark\bin> spark-class org.apache.spark.deploy.SparkSubmit --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 spark-internal

![start thrift server.png](start%20thrift%20server.png)

Test connection

    beeline> !connect jdbc:hive2://localhost:10000

![connect using beeline.png](connect%20using%20beeline.png)

## Catalog

The highest level abstraction in Spark SQL is the Catalog. The Catalog is an abstraction for the storage of metadata
about the data stored in your tables as well as other helpful things like database, tables, functions, and views. The
catalog is available in the `org.apache.spark.sql.catalog.Catalog` package and contains a number of helpful functions
for doing things like listing tables, databases, and functions

## Tables

To do anything useful with Spark SQL, you first need to define tables. Tables are logically equivalent to a DataFrame in
that they are a structure of data against which you run commands. We can join tables, filter them, aggregate them, and
perform different manipulations that we saw in previous chapters.

> The core different between tables and DataFrames
> - You define DataFrame in the scope of a programming language
> - You define tables within a database. This means it will belong to the _default_ database

### Spark-Managed Tables

Tables store two important pieces of information. The data within the tables as well as the data about the tables -
_metadata_. You can have Spark manage the metadata for a set of files as well as for the data. When you define a table
from files on disk, you are defining an **unmanaged table**. When you use `saveAsTable` on a DataFrame, you are creating
a **managed table** for which Spark will track of the relevant information.

This will read your table and write it out to a new location in Spark format. You can see this reflected in the new
explain plan. In the explain plan, you will also notice that this writes to the default Hive warehouse location. You can
set this by setting the `spark.sql.warehouse.dir` configuration to the directory of your choosing when you create your
SparkSession. By default, Spark sets this to `/user/hive/warehouse`

See tables in a specific database

    SHOW TABLES IN databaseName

> If you are running on a new cluster or local mode, this should return zero results

### Creating Tables

Something fairly unique to Spark is the capability of reusing the entire Data Source API within SQL. This means that you
do not need to define a table and then load data into it; Spark lets you create one on the fly. You can even specify all
sorts of sophisticated options when you read in a file

    spark.sql("DROP TABLE IF EXISTS flights")

    // read from json, data write and read from path
    spark.sql(
      """
        CREATE TABLE flights (
          DEST_COUNTRY_NAME STRING,
          ORIGIN_COUNTRY_NAME STRING,
          count LONG COMMENT "ayo i can comment here"
        ) USING JSON
        OPTIONS (path 'D:/repo_books/book_sparkthedefinitiveguide/data/flight-data/json/2015-summary.json');
      """
    ).show()

    spark.sql("SELECT * FROM flights").show()

> Hive users can also use the STORED AS syntax to specify that this should be a Hive table.

Create a table from a query

    CREATE TABLE flights_from_select USING parquet AS SELECT * FROM flights

Create a table only if it does not currently exist

    CREATE TABLE IF NOT EXISTS flights_from_select
        AS SELECT * FROM flights

Control the layout of data by writing out a partitioned dataset

    // partitioned, output in spark-warehouse
    spark.sql(
      """
        CREATE TABLE partitioned_flights USING parquet PARTITIONED BY (DEST_COUNTRY_NAME)
          AS SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM flights LIMIT 5;
      """
    )

> These tables will be available in Spark even through sessions; temporary tables do not currently exist in Spark. You
> must create a temporary view, which we demonstrate later in this chapter.

### Creating External Tables


