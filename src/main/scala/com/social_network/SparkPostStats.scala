package com.social_network

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.{DoubleType, StructField}
import org.apache.spark.sql.types.*
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*
import org.apache.hadoop.fs.{FileSystem, Path}
import com.social_network.SocialNetwortUtils._
import java.io.File


object SparkPostStats {

  def main(args: Array[String]): Unit = {
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = SparkSession.builder
      .appName("Social Network retweet analysis")
      .master(sys.env.getOrElse("SPARK_MASTER_URL", "local[*]"))
      .getOrCreate()

    try {

      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(hadoopConf)
      val avroPath = s"/opt/spark-data/input/socialNetwork/"

      val filesStatus = fs.listStatus(new Path(avroPath))

      val avroFiles = filesStatus
        .filter(_.getPath.getName.endsWith(".avro"))
        .map(_.getPath.toString)

      val dfs: Map[String, DataFrame] = avroFiles.map { file =>
        val name = new File(file).getName.stripSuffix(".avro")
        val df = spark
          .read
          .format("avro")
          .option("header", "true")
          .load(file)
        name -> df
      }.toMap

      dfs.foreach { case (name, df) =>
        println(s"Schema for $name:")
        df.printSchema()
      }

      val wavesFiltered = retweet_wave_filter(dfs("RETWEET"))

      val countRetweets = count_retweets(dfs("RETWEET"), "USER_ID", "MESSAGE_ID")
      
      

      countRetweets.orderBy(col("count").desc).show(30, truncate = false)
      
    } catch {
      case e: Exception =>
        println("Error: " + e.getMessage)
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

}
