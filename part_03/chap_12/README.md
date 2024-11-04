# Chapter 12. Resilient Distributed Datasets (RDDs)

There are times when higher-level manipulation will not meet the business or engineering problem you are trying to
solve. For those cases, you might need to use Spark's lower-level APIs, specifically the Resilient Distributed Dataset (
RDD), the SparkContext, and distributed _shared variables_ like accumulators and broadcast variables.

## Low-Level APIs

There are two sets of low-level APIs:

- **RDDs** - for manipulating distributed data
- **Broadcast variables and accumulators** - for distributing and manipulating distributed shared variables
    - Broadcast Variables: Là biến toàn cục được chia sẻ cho tất cả các node trong cluster, thường dùng cho các dữ liệu
      bất biến như cấu hình hay dữ liệu tham chiếu. Các biến này giúp giảm bớt số lần truyền tải dữ liệu qua mạng.
    - Accumulators: Là biến có khả năng tích lũy giá trị từ nhiều thao tác trên nhiều node. Chúng thường được dùng để
      thu thập thông tin về các phép tính (ví dụ: đếm số lượng các lần lặp) nhưng không ảnh hưởng đến việc tính toán
      chính.

### When to use the Low-Level APIs

- Need some functionality that can not find in the higher-level APIs
- Need to maintain some legacy codebase written using RDDs
- Need to do some custom shared variable manipulation

When you’re calling a DataFrame transformation, it actually just becomes a set of RDD transformations. This
understanding can make your task easier as you begin debugging more and more complex workloads

### How to use the Low-Level APIs

A `SparkContext` is the entry point for low-level API functionality. You access it through the `SparkSession`, which is
the tool you use to perform computation across a Spark cluster.

## About RDDs

In short, an RDD represents an immutable, partitioned collection of records that can be operated on in parallel. Unlike
DataFrames though, where each record is a structured row containing fields with a known schema, in RDDs the records are
just Java, Scala, or Python objects of the programmer’s choosing.

RDDs give you complete control because every record in an RDD is just a Java or Python object. You can store anything
you want in these object, in any format you want. Every manipulation and interaction between values must be defined by
hand, meaning that you must “reinvent the wheel” for whatever task you are trying to carry out. Also, optimizations are
going to require much more manual work, because Spark does not understand the inner structure of your records as it does
with the Structured APIs. For instance, Spark’s Structured APIs automatically store data in an optimized, compressed
binary format, so to achieve the same space-efficiency and performance, you’d also need to implement this type of format
inside your objects and all the low-level operations to compute over it. Likewise, optimizations like reordering filters
and aggregations that occur automatically in Spark SQL need to be implemented by hand. For this reason and others, we
highly recommend **using the Spark Structured APIs when possible**

The RDD API is similar to the `Dataset` except that RDDs are not stored in, or manipulated with, the structured data
engine. However, it is trivial to convert back and forth between RDDs and Datasets, so you can use both APIs to take
advantage of each API’s strengths and weaknesses

### Types of RDDs

As a user, you will likely only be creating two types of RDDs

- The "generic" RDD type
- The key-value RDD that provides additional functions, such as aggregating by key

Each RDD is characterized by five main properties:

- A list of partitions
- A function for computing each split
- A list of dependencies on other RDDs
- Optionally, a `Partitioner` for key-value RDDs (partitioning with key)
- Optionally, a list of preferred locations on which to compute each split

RDDs follow the exact same Spark programming paradigms that we saw in earlier chapters. They provide transformations,
which evaluate lazily, and actions, which evaluate eagerly, to manipulate data in a distributed fashion

The is no concept of "rows" in RDDs, individual records are just raw Java/Scala/Python objects, and you manipulate those
manually instead of tapping into the repository of functions that you have in the Structured APIs

> Python < Scala/Java. Python can lose a substantial amount of performance when using RDDs.

## Creating RDDs

### Interoperating Between DataFrames, Datasets, and RDDs

One of the easiest ways to get RDDs is from an existing DataFrame or Dataset: just use the `rdd` method on any of these
data types

    // in Scala: converts a Dataset[Long] to RDD[Long]
    spark.range(500).rdd

To operate on this data, you will need to convert this `Row` object to the correct data type or extract values out of it

    // in Scala
    spark.range(10).toDF().rdd.map(rowObject => rowObject.getLong(0))

    # in Python
    spark.range(10).toDF("id").rdd.map(lambda row: row[0])

Create a DataFrame or Dataset from a RDD

    spark.range(10).rdd.toDF()

### From a Local Collection

To create an RDD from a collection, you will need to use the `parallelize` method on a `SparkContext` (within a
SparkSession). This turns a single node collection into a parallel collection. When creating this parallel collection,
you can also explicitly state the number of partitions into which you would like to distribute this array

    val myCollection = "Spark The Definitive Guide: Big Data Processing Made Simple"
      .split(" ")
    val words = spark.sparkContext.parallelize(myCollection, 2)
    words.setName("myWords")
    println(words.name)

### From Data Source

Read a text file line by line

    spark.sparkContext.textFile("/some/path/withTextFiles")

This creates an RDD for which each record in the RDD represents a line in that text file or files. Alternatively, you
can read in data for which each text file should become a single record. The use case here would be where each file is a
file that consists of a large JSON object or some document that you will operate on as an individual

    spark.sparkContext.wholeTextFiles("/some/path/withTextFiles")

## Manipulating RDDs

To demonstrate some data manipulation, let’s use the simple RDD (words) we created previously to define some more
details.

## Transformations

For the most part, many transformations mirror the functionality that you find in the Structured APIs. Just as you do
with DataFrames and Datasets, you specify _transformations_ on one RDD to create another.

### distinct

    words.distinct().count()

### filter

    // in Scala
    private def startsWithS(a: String): Boolean = {
        a.startsWith("S") // return True/False
    }

    // in Scala
    words.filter(word => startsWithS(word)).collect().foreach(println)

### map

Mapping is the same operation in Chapter 11. You specify a function that returns the value that you want, given the
correct input. You then apply that, record by record.

    // in Scala
    val words2 = words.map(word => (word, word(0), word.startsWith("S")))
    words2.collect().foreach(println)

    /*
        (Spark,S,true)
        (The,T,false)
        (Definitive,D,false)
        (Guide:,G,false)
        (Big,B,false)
        (Data,D,false)
        (Processing,P,false)
        (Made,M,false)
        (Simple,S,true)
    */

    # in Python
    words2 = words.map(lambda word: (word, word[0], word.startswith("S")))

Subsequently filter on this by selecting the relevant Boolean value in a new function

    // get only true row
    // in Scala
    words2.filter(record => record._3).take(5)

    # in Python
    words2.filter(lambda record: record[2]).take(5)

#### flatmap

Each current row should return multiple rows. `flatMap` requires that the output of the map function be an iterable that
can be expanded:

    words.flatMap(word => word.toSeq).collect().foreach(println)
    words.flatMap(lambda word: list(word)).take(5)

### sort

To sort an RDD you must use the `sortBy` method, and just like any other RDD operation, you do this by specifying a
function to extract a value from the objects in your RDDs and then sort based on that

    // in Scala
    words.sortBy(word => word.length() * -1)

### Random Splits

Randomly split an RDD into an Array of RDDs by using the `randomSplit` method, which accepts an Array of weights and a
random seed

    // random split
    val splits = words.randomSplit(Array(0.5, 0.5))
    splits.zipWithIndex.foreach { case (rdd, index) =>
      println(s"Contents of split $index:")
      rdd.collect().foreach(println)
    }

## Actions