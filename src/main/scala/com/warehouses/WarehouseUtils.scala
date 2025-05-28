package com.warehouses

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object WarehouseUtils {

  def calculate_recent_amount(df : DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("positionId").orderBy(col("amount recorded at").desc)

    df
      .withColumn("row_num", row_number().over(windowSpec))
      .filter(col("row_num") === 1)
      .drop("row_num")
      .withColumnRenamed("amount recorded at", "Last amount recorded at")
      .withColumnRenamed("amount", "Current Amount")
  }

  def stats_per_warehouse(df: DataFrame): DataFrame = {
    df.groupBy("warehouse", "product")
      .agg(
        round(avg("amount")).as("avg_amount"),
        max("amount").as("max_amount"),
        min("amount").as("min_amount")
      )
  }
}
