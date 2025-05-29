package com.scripts

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.{DoubleType, StructField}
import org.apache.spark.sql.types.*
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.Path

object CsvToAvro {
  def main(args: Array[String]): Unit = {

    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = SparkSession.builder
      .appName("SocialNetworkCsvToAvro")
      .master(sys.env.getOrElse("SPARK_MASTER_URL", "local[*]"))
      .getOrCreate()

    try {
      val basePath = "/opt/spark-data/input/socialNetwork"

      val csvFiles = Seq(
        "USER_DIR.csv",
        "MESSAGE_DIR.csv",
        "MESSAGE.csv",
        "RETWEET.csv"
      )

      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)

      csvFiles.foreach { fileName =>
        val csvPath = s"$basePath/$fileName"
        val avroOutputPath = s"$basePath/${fileName.stripSuffix(".csv")}.avro"

        val fileExists = fs.exists(new Path(csvPath))
        if (!fileExists) throw new Exception(s"Input file not found: $csvPath")

        println(s"Reading CSV from: $csvPath")
        val df = spark.read.option("header", "true").csv(csvPath)

        println(s"Writing AVRO to: $avroOutputPath")
        df.write
          .mode("overwrite")
          .format("avro")
          .save(avroOutputPath)
      }

    } catch {
      case e: Exception =>
        println(s"An error occurred: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
    }
  }
}