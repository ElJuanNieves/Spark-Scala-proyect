package com.social_network

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.{DoubleType, StructField}
import org.apache.spark.sql.types.*
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*
import org.apache.hadoop.fs.{FileSystem, Path}
import com.social_network.SparkNetworkUtils._
import java.io.File


/**
 * Social Network Post Analytics Application
 * 
 * Analyzes the propagation of retweets through social networks by tracking
 * how messages spread in waves from original posters to their followers.
 * This application identifies deep propagation patterns where messages
 * reach multiple levels of the social graph.
 */
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
      /**
       * Data Source Configuration
       * Set up file system access and identify input data location
       */
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(hadoopConf)
      val avroPath = s"/opt/spark-data/input/socialNetwork/"

      val filesStatus = fs.listStatus(new Path(avroPath))

      /**
       * Data Loading
       * Load all available Avro files containing social network data
       */
      val avroFilePaths = filesStatus
        .filter(_.getPath.getName.endsWith(".avro"))
        .map(_.getPath.toString)

      val dataFrameMap: Map[String, DataFrame] = avroFilePaths.map { file =>
        val name = new File(file).getName.stripSuffix(".avro")
        val df = spark
          .read
          .format("avro")
          .option("header", "true")
          .load(file)
        name -> df
      }.toMap

      /**
       * Retweet Wave Analysis
       * Analyze how messages propagate through the network in waves
       * - Wave 0: Original message authors
       * - Wave 1: Direct followers who retweet
       * - Wave 2+: Secondary/tertiary propagation through the network
       */
      val retweetWaveResults = retweetWaveFilter(dataFrameMap("MESSAGE"), dataFrameMap("RETWEET"), 3)
      println("--------Top Users with most retweets on first 3 waves-------------- ")
      /*******************Top Users with most retweets*******************/ 
      /* Adjust the filter for you to get different info and stats*/ 
      retweetWaveResults.orderBy(desc("Count")).filter(col("depth") <= 2).show(40)

    } catch {
      case e: Exception =>
        println("Error: " + e.getMessage)
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

}
