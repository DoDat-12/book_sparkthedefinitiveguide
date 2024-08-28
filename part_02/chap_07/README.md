# Chapter 7. Aggregations

Aggregating is the act of collecting something together and is a cornerstone of big data analytics. In an aggregation,
you will specify a _key_ or _grouping_ and an _aggregation function_ that specifies how you should transform one or two
columns. This function must produce one result for each group, given multiple input values

Spark allows us to create the following groupings types

- Simplest grouping: Summarize a complete DataFrame by performing an aggregation in a select statement
- A "group by" to specify one or more keys as well as one or more aggregation functions to transform the value columns
- A "window" same as "group by". However, the rows input to the functions are somehow related to the current row
- A “grouping set,” which you can use to aggregate at multiple different levels. Grouping sets are available as a
  primitive in SQL and via rollups and cubes in DataFrames.
- A “rollup” makes it possible for you to specify one or more keys as well as one or more aggregation functions to
  transform the value columns, which will be summarized hierarchically.
- A “cube” allows you to specify one or more keys as well as one or more aggregation functions to transform the value
  columns, which will be summarized across all combinations of columns.

## Aggregation Functions

You can find most aggregation functions in the org.apache.spark.sql.functions package

### count

    df.select(count("StockCode")).show()

### countDistinct

The number of unique groups

    df.select(countDistinct("StockCode")).show()

### approx_count_distinct

Often, exact distinct count with large datasets is irrelevant. There are times when an approximation to a certain degree
of
accuracy will work just fine, and for that, you can use the approx_count_distinct function

    df.select(approx_count_distinct("StockCode", 0.1)).show()

Another parameter specify the maximum estimation error allowed

### first and last

Get the first and last values from a DataFrame

    df.select(first("StockCode"), last("StockCode")).show()

### min and max

    df.select(
      min("Quantity").alias("min"),
      max("Quantity").alias("max")
    ).show()

### sum

Add all the values in a row

    df.select(sum("Quantity")).show()

### sumDistinct

    df.select(sum_distinct(col("Quantity"))).show()

### avg

    df.select(avg("Quantity").alias("avg_purchases")).show()

### Variance and Standard Deviation

Spark performs the formula for the sample standard deviation or variance if you use the `variance` or `stddev` functions

    df.select(
      var_pop("Quantity"),
      var_samp("Quantity"),
      stddev_pop("Quantity"),
      stddev_samp("Quantity")
    ).show(truncate=false)

### skewness and kurtosis

Skewness measures the asymmetry of the values in your data around the mean, whereas kurtosis is a measure of the tail of
data

    df.select(
      skewness("Quantity"),
      kurtosis("Quantity")
    ).show(truncate=false)

### Covariance and Correlation

Compare the interactions of the values in two difference columns together

- `cov` function for covariance
- `corr` function for correlation

      df.select(
        corr("InvoiceNo", "Quantity"),
        covar_samp("InvoiceNo", "Quantity"),
        covar_pop("InvoiceNo", "Quantity")
      ).show()

> Correlation measures the Pearson correlation coefficient, which is scaled between –1 and +1. The covariance is scaled
> according to the inputs in the data.

## Aggregating to Complex Types

For example, we can collect a list of values present in a given column or only the unique values by collecting to a set.

    df.agg(collect_set("Country"), collect_list("Country")).show(truncate=false)

## Grouping

First we specify the column(s) on which we would like to group, and then we specify the aggregation(s). The first step
returns a `RelationalGroupedDataset`, and the second step returns a `DataFrame`

    df.groupBy("InvoiceNo", "CustomerId").count().show()

### Grouping with Expressions

Usually we prefer to use the count function. Rather than passing that function as an expression into a select statement,
we specify it as within agg. This makes it possible for you to pass-in arbitrary expressions that just need to have some
aggregation specified

    df.groupBy("InvoiceNo").agg(
      count("Quantity").alias("quan"),
      expr("count(Quantity)")
    ).show()

### Grouping with Maps

Sometimes, it can be easier to specify your transformations as a series of Maps for which the key is the column, and the
value is the aggregation function (as a string) that you would like to perform. You can reuse multiple column names if
you specify them inline

    // in Scala
    df.groupBy("InvoiceNo").agg("Quantity"->"avg", "Quantity"->"stddev_pop").show()

    # in Python
    df.groupBy("InvoiceNo").agg(expr("avg(Quantity)"), expr("stddev_pop(Quantity)")).show()

