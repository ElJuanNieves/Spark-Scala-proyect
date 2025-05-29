package com.social_network

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.{DoubleType, StructField}
import org.apache.spark.sql.types.*
import org.apache.spark.sql.expressions.Window


object SparkPostStats {

  object SparkWarehouses {

    def main(args: Array[String]): Unit = {

      import org.apache.log4j.{Level, Logger}
      Logger.getLogger("org").setLevel(Level.WARN)
      Logger.getLogger("akka").setLevel(Level.WARN)

      val spark = SparkSession.builder
        .appName("SocialNetworkAnalysis")
        .master(sys.env.getOrElse("SPARK_MASTER_URL", "local[*]"))
        .getOrCreate()

      try {
        /*
          ****************Read and set data files path**********************
         */
        val inputAmountsPath = "/opt/spark-data/input/warehouses/amounts.csv"
        println(s"Reading Amounts CSV from: $inputAmountsPath")

        val inputPositionPath = "/opt/spark-data/input/warehouses/positions.csv"
        println(s"Reading Position CSV from: $inputPositionPath")

        val outputCurrentAmountsPath = "/opt/spark-data/output/warehouses/CurrentAmounts"
        val outputWarehouseStatsPath = "/opt/spark-data/output/warehouses/WarehouseStats"

        val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)

        // First check if the file exists using shell command
        val amountsPath = new org.apache.hadoop.fs.Path(inputAmountsPath)
        if (!fs.exists(amountsPath)) {
          throw new Exception(s"Input file not found at: $inputAmountsPath")
        }

        val positionPath = new org.apache.hadoop.fs.Path(inputPositionPath)
        if (!fs.exists(positionPath)) {
          throw new Exception(s"Input file not found at: $inputPositionPath")
        }
        /*
          ****************Load data**********************
         */


        /*
         ***********************Process Dataframes*************************************
         */

        /*
        *************************Writes output****************************************
         */



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

}
