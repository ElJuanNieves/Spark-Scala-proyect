package com.warehouses

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

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
      // Use absolute path without file:// protocol
      val inputPath = "/opt/spark-data/input/warehouses.csv"
      println(s"Reading CSV from: $inputPath")

      // First check if the file exists using shell command
      val path = new org.apache.hadoop.fs.Path(inputPath)
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      
      if (!fs.exists(path)) {
        throw new Exception(s"Input file not found at: $inputPath")
      }

      // Load data from CSV file
      val warehousesDF = spark
        .read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(inputPath)

      println("Original Warehouse Data:")
      warehousesDF.show()

      // Perform some analysis
      val analysisDF = warehousesDF
        .withColumn("capacity_status", 
          when(col("capacity") > 1000, "Large")
          .when(col("capacity") > 500, "Medium")
          .otherwise("Small")
        )
        .withColumn("processed_date", current_date())

      println("Processed Warehouse Data:")
      analysisDF.show()
      
      // Save the results
      val outputPath = "/opt/spark-data/output/processed_warehouses"
      println(s"Writing results to: $outputPath")

      analysisDF.write
        .format("csv")
        .mode("overwrite")
        .option("header", "true")
        .save(outputPath)

      println(s"Data has been processed and saved to $outputPath")
    
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
