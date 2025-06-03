package com.warehouses

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.{DoubleType, StructField}
import org.apache.spark.sql.types.*
import org.apache.spark.sql.expressions.Window
import com.warehouses.WarehouseUtils.*


/**
 * Main application for warehouse inventory analysis using Apache Spark.
 * This application processes warehouse position and amount data to:
 * 1. Calculate the most recent amounts for each position
 * 2. Generate statistical summaries for each warehouse and product
 * 
 * The results are saved as CSV files to the specified output paths.
 */
object SparkWarehouses {

  def main(args: Array[String]): Unit = {

    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = SparkSession.builder
      .appName("WarehouseAnalysis")
      .master(sys.env.getOrElse("SPARK_MASTER_URL", "local[*]"))
      .getOrCreate()

    try {
      /**
       * Data file paths configuration
       */
      val inputAmountsPath = "/opt/spark-data/input/warehouses/amounts.csv"
      println(s"Reading Amounts CSV from: $inputAmountsPath")

      val inputPositionPath = "/opt/spark-data/input/warehouses/positions.csv"
      println(s"Reading Position CSV from: $inputPositionPath")

      val outputCurrentAmountsPath = "/opt/spark-data/output/warehouses/CurrentAmounts"
      val outputWarehouseStatsPath = "/opt/spark-data/output/warehouses/WarehouseStats"

      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      
      val amountsPath = new org.apache.hadoop.fs.Path(inputAmountsPath)
      if (!fs.exists(amountsPath)) {
        throw new Exception(s"Input file not found at: $inputAmountsPath")
      }

      val positionPath = new org.apache.hadoop.fs.Path(inputPositionPath)
      if (!fs.exists(positionPath)) {
        throw new Exception(s"Input file not found at: $inputPositionPath")
      }
      
      /**
       * Schema definition and data loading
       */

      import spark.implicits._

      val amountSchema = StructType(Array(
        StructField("positionId", LongType, nullable = false),
        StructField("amount", DoubleType, nullable = false),
        StructField("eventTime", LongType, nullable = false)
      ))
      
      val positionSchema = StructType(Array(
        StructField("positionId", LongType, nullable = false),
        StructField("warehouse",  StringType, nullable = false),
        StructField("product", StringType, nullable = false),
        StructField("eventTime", LongType, nullable = false)
      ))

      val amountsDataFrame = spark
        .read
        .format("csv")
        .option("header", "true")
        .schema(amountSchema)
        .load(inputAmountsPath)

      println("Original Amounts Data:")
      amountsDataFrame.show()

      val positionsDataFrame = spark
        .read
        .format("csv")
        .option("header", "true")
        .schema(positionSchema)
        .load(inputPositionPath)

      val cleanAmountsDataFrame = amountsDataFrame.withColumnRenamed("eventTime", "amountRecordedAt")
      val cleanPositionsDataFrame = positionsDataFrame.withColumnRenamed("eventTime", "positionCreatedAt")

      val amountPerPosition = cleanAmountsDataFrame
        .join(cleanPositionsDataFrame, Seq("positionId"), "inner")
        
      /**
       * Data processing and analysis
       */
      val mostRecentAmount: DataFrame = calculateRecentAmount(amountPerPosition)
      val statsPerWarehouse: DataFrame = calculateWarehouseStats(amountPerPosition)

      println("Most recent amounts by position:")
      mostRecentAmount.show()
      val orderedStats = statsPerWarehouse.orderBy("warehouse", "product")
      println("Stats by Warehouse Positions:")
      orderedStats.show()

      /**
       * Output generation - write results to CSV files
       */

      mostRecentAmount
        .orderBy("positionId")
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(outputCurrentAmountsPath)


      orderedStats
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(outputWarehouseStatsPath)

      println("Data successfully collected")

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
