# Chapter 6. Working with Different Types of Data

## Where to Look for APIs

https://spark.apache.org/docs/latest/api/scala/index.html
https://spark.apache.org/docs/3.1.3/api/python/reference/index.html

    val df = spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/data/retail-data/by-day/2010-12-01.csv")
    df.printSchema()
    df.createOrReplaceTempView("dfTable")

    root
    |-- InvoiceNo: string (nullable = true)
    |-- StockCode: string (nullable = true)
    |-- Description: string (nullable = true)
    |-- Quantity: integer (nullable = true)
    |-- InvoiceDate: timestamp (nullable = true)
    |-- UnitPrice: double (nullable = true)
    |-- CustomerID: double (nullable = true)
    |-- Country: string (nullable = true)

## Converting to Spark Types

Convert native types to Spark types using the `lit` function

    // in Scala
    import org.apache.spark.sql.functions.lit
    df.select(lit(5), lit("five"), lit(5.0))

## Working with Booleans

    import org.apache.spark.sql.functions.col
    df.where(col("InvoiceNo").equalTo(536365))  // or col("InvoiceNo") === 536365
      .select("InvoiceNo", "Description")
      .show(5, false)

Cleanest

    df.where("InvoiceNo = 536365")
      .show(5, false)  // truncate = false

    df.where("InvoiceNo <> 536365")
      .show(5, false)

Specify Boolean expressions with multiple parts: chain together and filters as a sequential filter

> Spark will flatten all of these filters into one statement and perform the filter at the same time, create the `and`
> statement for us. `or` statements need to be specified in the same statement.

    // in Scala
    val priceFilter = col("UnitPrice") > 600
    val descripFilter = col("Description").contains("POSTAGE") // conditions
    df.where(col("StockCode").isin("DOT"))
      .where(priceFilter.or(descripFilter))
      .show()

![boolean example.png](boolean%20example.png)

isin: https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.Column.isin.html

    # in Python
    from pyspark.sql.functions import instr
    priceFilter = col("UnitPrice") > 600
    descripFilter = instr(df.Description, "POSTAGE") >= 1
    df.where(df.StockCode.isin("DOT")).where(priceFilter | descripFilter).show()

instr: https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.instr.html?highlight=instr#pyspark.sql.functions.instr

To filter a `DataFrame`, you can also just specify a Boolean column

    // in Scala
    val DOTCodeFilter = col("StockCode") === "DOT"
    val priceFilter = col("UnitPrice") > 600
    val descripFilter = col("Description").contains("POSTAGE")
    df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descripFilter)))
      .where("isExpensive")
      .select("unitPrice", "isExpensive").show(5)

    # in Python
    from pyspark.sql.functions import instr    

    DOTCodeFilter = col("StockCode") == "DOT"
    priceFilter = col("UnitPrice") > 600
    descripFilter = instr(col("Description"), "POSTAGE")
    df.withColumn("isExpensive", DOTCodeFilter & (priceFilter | descripFilter))
      .where("isExpensive")
      .select("unitPrice", "isExpensive").show(5)

It's often easier to just express filter as SQL statements than using the programmatic `DataFrame` interface

    df.withColumn("isExpensive", not(col("UnitPrice").leq(250)))

    df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))

Null-safe equivalent test (pyspark)

    df.where(col("Description").eqNullSafe("hello")).show()

eqNullSafe: https://youtu.be/-qx6bF-T6dw

## Working with Numbers

`pow` function

    import org.apache.spark.sql.functions.{expr, pow}
    val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
    df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity"))
      .show()
    // fabricatedQuantity is an expression so CustomerId needs to be expression too

We can do all of this as a SQL expression

    df.selectExpr(
        "CustomerId",
        "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity"
    ).show(2)

Rounding

    // in Scala
    import org.apache.spark.sql.functions.{round, bround}
    df.select(round(col("UnitPrice"), 1).alias("rounded"), col("unitPrice").show(5)
    // bround round down

Compute the correlation of two columns. For example, we can see the Pearson correlation coefficient for two columns to
see if cheaper things are typically bought in greater quantities

    df.stat.corr("Quantity", "UnitPrice")
    df.select(corr("Quantity", "UnitPrice")).show()

`describe` method computes summary statistics for a column or set of column

    df.describe().show()

    +-------+------------------+------------------+------------------+
    |summary|          Quantity|         UnitPrice|        CustomerID|
    +-------+------------------+------------------+------------------+
    |  count|              3108|              3108|              1968|
    |   mean| 8.627413127413128| 4.151946589446603|15661.388719512195|
    | stddev|26.371821677029203|15.638659854603892|1854.4496996893627|
    |    min|               -24|               0.0|           12431.0|
    |    max|               600|            607.49|           18229.0|
    +-------+------------------+------------------+------------------+

    import org.apache.spark.sql.functions.{count, mean, stddev_pop, min, max}

There are a number of statistical functions available in the StatFunction Package (accessible using `stat`)

Calculate approximate quantiles of data

    val colName = "UnitPrice"
    val quantileProbs = Array(0.5)
    val relError = 0.05
    df.stat.approxQuantile("UnitPrice", quantileProbs, relError)

Cross-tabulation

    // in Scala
    df.stat.crosstab("StockCode", "Quantity").show()

    # in Python
    df.stat.crosstab("StockCode", "Quantity").show()

Frequent item pairs

    // in Scala
    df.stat.freqItems(Seq("StockCode", "Quantity")).show()

    # in Python
    df.stat.freqItems(["StockCode", "Quantity"]).show()

Add unique ID to each row by using `monotonically_increasing_id` function

    // in Scala
    import org.apache.spark.sql.functions.monotonically_increasing_id
    df.select(monotonically_increasing_id()).show(2)

    # in Python
    from pyspark.sql.functions import monotonically_increasing_id
    df.select(monotonically_increasing_id()).show(2)

## Working with Strings

The `initcap` function: Capitalize every word in a given string when that word is separated from another by a space

    df.select(initcap(col("Description"))).show()

Cast strings in uppercase and lowercase

    df.select(
        col("Description"),
        lower(col("Description"),
        upper(col("Description")
    ).show()

Adding or removing spaces around a string: `lpad`, `ltrim`, `rpad`, `rtrim`, `trim`

### Regular Expressions

Regular expressions give the user an ability to specify a set of rules to use to either extract values from a string or
replace them with some other values

Two key functions in Spark: `regexp_extract` and `regexp_replace`
    
    // scala
    import org.apache.spark.sql.functions.regexp_replace
    val simpleColors = Seq("black", "white", "red", "green", "blue")
    val regexString = simpleColors.map(_.toUpperCase).mkString("|")
    df.select(
        regexp_replace(col("Description"), regexString, "COLOR").alias("color_clean"),
        col("Description")
    ).show(2)

    // python
    from pyspark.sql.functions import regexp_place
    regex_string = "BLACK|WHITE|RED|GREEN|BLUE"
    df.select(
        regexp_replace(col("Description"), regex_string, "COLOR").alias("color_clean")
        col("Description")
    ).show(2)

    +--------------------+--------------------+
    |         color_clean|         Description|
    +--------------------+--------------------+
    |COLOR HANGING HEA...|WHITE HANGING HEA...|
    | COLOR METAL LANTERN| WHITE METAL LANTERN|
    +--------------------+--------------------+
    // Replace WHITE with COLOR

Replace given characters with other characters

    df.select(translate(col("Description"), "LEET", "1337"), col("Description")).show(2)

    +----------------------------------+--------------------+
    |translate(Description, LEET, 1337)|         Description|
    +----------------------------------+--------------------+
    |              WHI73 HANGING H3A...|WHITE HANGING HEA...|
    |               WHI73 M37A1 1AN73RN| WHITE METAL LANTERN|
    +----------------------------------+--------------------+

Using regexp_extract

    val regexString = simpleColors.map(_.toUpperCase).mkString("(", "|", ")") // python: regexString = "(BLACK|WHITE|RED|GREEN|BLUE)"
    df.select(
        regexp_extract(col("Description"), regexString, 1).alias("color_clean"),
        col("Description")
    ).show(2)

Check for existence

    col("Description").contains("BLACK")

## Working with Dates and Timestamps

Get the current date and the current timestamp

    .withColumn("today", current_date())
    .withColumn("now", current_timestamp())

Add and subtract days

    dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show()

The `datediff` function that returns the number of days in between two dates

    select(datediff(col("week_ago"), col("today"))

The `months_between` gives the number of month between two dates

    select(months_between(col("start"), col("end"))).show()

The `to_date` function converts a string to a date, optionally with a specified format

    spark.range(5).withColumn("date", lit("2017-01-01"))
        .select(to_date(col("date"))).show(1)

> If cannot parse the date, it returns `null`

    dateDF.select(to_date(lit("2016-20-12")),to_date(lit("2017-12-11"))).show(1)

    +-------------------+-------------------+
    |to_date(2016-20-12)|to_date(2017-12-11)|
    +-------------------+-------------------+
    |               null|         2017-12-11|
    +-------------------+-------------------+

Date format according to: https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html

    val dateFormat = "yyyy-dd-MM"
    spark.range(1).select(
        to_date(lit("2017-12-11"), dateFormat).alias("date"),
        to_date(lit("2017-20-12"), dateFormat).alias("date2")
    ).show()

`to_timestamp` always requires a format to be specified

    select(to_timestamp(col("date"), dateFormat))

    +----------------------------------+
    |to_timestamp(`date`, 'yyyy-dd-MM')|
    +----------------------------------+
    |               2017-11-12 00:00:00|
    +----------------------------------+

## Working with Nulls in Data