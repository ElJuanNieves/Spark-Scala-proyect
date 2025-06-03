package com.warehouses

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
 * Utility object for warehouse data analysis.
 * Provides functions for calculating the most recent amounts for positions
 * and generating statistical summaries of warehouse inventory.
 */
object WarehouseUtils {

  /**
   * Calculates the most recent amount for each position based on the event timestamp.
   * Uses a window function to find the latest record for each position.
   *
   * @param df DataFrame containing position amount data with timestamps
   * @return DataFrame with only the most recent amount for each position
   */
  def calculateRecentAmount(df: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("positionId").orderBy(col("amountRecordedAt").desc)

    df
      .withColumnRenamed("amount recorded at", "amountRecordedAt")
      .withColumn("row_num", row_number().over(windowSpec))
      .filter(col("row_num") === 1)
      .drop("row_num")
      .withColumnRenamed("amountRecordedAt", "lastAmountRecordedAt")
      .withColumnRenamed("amount", "currentAmount")
  }

  /**
   * Calculates statistical summaries for product amounts grouped by warehouse and product.
   * Computes average, maximum, and minimum amounts for each warehouse-product combination.
   *
   * @param df DataFrame containing warehouse position data with amount information
   * @return DataFrame with aggregated statistics for each warehouse-product combination
   */
  def calculateWarehouseStats(df: DataFrame): DataFrame = {
    df.groupBy("warehouse", "product")
      .agg(
        round(avg("amount")).as("averageAmount"),
        max("amount").as("maximumAmount"),
        min("amount").as("minimumAmount")
      )
  }
}
