# Chapter 2. A Gentle Introduction to Spark

## Spark's Basic Architecture

Single machines do not have enough power and resources to perform computations on huge amounts of information (or the
user probably does not have the time to wait for the computation to finish). A cluster, or group, of computers, pools
the resources of many machines together, giving us the ability to use all the cumulative resources as if they were a
single computer. Spark manages and coordinates the execution of tasks on data across a cluster of computers.

### Spark Applications

Spark Applications consist of a _driver_ process and a set of _executor_ processes.

The _driver_ process runs your `main()` function, sits on a node in the cluster:

- Maintaining information about Spark Application

- Responding to a user's program or input

- Analyzing, distributing, and scheduling work across the executors

> Driver process maintains all relevant information during the lifetime of the application

The _executors_ carry out the work that the driver assigns them:

- Executing code assigned to it by the driver

- Reporting the state of the computation on that executor back to the driver node

![spark application architecture.png](spark%20application%20architecture.png)

Spark, in addition to its cluster mode, also has a _local mode_. The driver and executors are simply processes, which
means that they can live on the same machine or different machines. In local mode, the driver and executors run (as
threads) on your individual computer instead of a cluster.

> Spark employs a cluster manager that keeps track of the resources available. The driver process is responsible for
> executing the driver program’s commands across the executors to complete a given task.

## The SparkSession

![start sparksession.png](start%20sparksession.png)

We created a _DataFrame_ with one column containing 1,000 rows with values from 0 to 999. This range of numbers
represents a _distributed collection_. When run on a cluster, each part of this range of numbers exists on a different
executor. This is a Spark DataFrame
