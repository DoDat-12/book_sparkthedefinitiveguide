package spark.chap09

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("chap09-example")
      .master("local")
      .getOrCreate()

    // csv reader
    val myManualSchema = new StructType(Array(
      StructField("DEST_COUNTRY_NAME", StringType, nullable = true),
      StructField("ORIGIN_COUNTRY_NAME", StringType, nullable = true),
      StructField("count", LongType, nullable = false)
    ))

    val csvDF = spark.read.format("csv")
      .option("header", "true")
      .option("mode", "FAILFAST")
      .schema(myManualSchema)
      .load("D:\\repo_books\\book_sparkthedefinitiveguide\\data\\flight-data\\csv\\2010-summary.csv")

    // csv writer to tsv
    csvDF.write.format("csv")
      .mode("overwrite")
      .option("sep", "\t")
      .save("D:\\repo_books\\spark_chap09\\output\\my-tsv-files")  // folder

    // json reader
    spark.read.format("json")
      .option("mode", "FAILFAST")
      .schema(myManualSchema)
      .load("D:\\repo_books\\book_sparkthedefinitiveguide\\data\\flight-data\\json\\2010-summary.json")
      .show(5)

    // json writer
    csvDF.write.format("json")
      .mode("overwrite")
      .save("D:\\repo_books\\spark_chap09\\output\\my-json-files")
  }
}