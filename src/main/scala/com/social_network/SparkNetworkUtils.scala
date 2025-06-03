/*
 */
package com.social_network

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, StructField}
import org.apache.spark.sql.types.*
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
 * Utility object for social network data processing with Spark.
 * Provides schema definitions and methods for analyzing retweet propagation
 * through social networks.
 */
object SparkNetworkUtils {

  val messageSchema = StructType(Array(
    StructField("USER_ID", LongType, nullable = false),
    StructField("MESSAGE_ID", LongType, nullable = false)
  ))
  val messageDirectorySchema = StructType(Array(
    StructField("MESSAGE_ID", LongType, nullable = false),
    StructField("TEXT", StringType, nullable = false)
  ))
  val retweetSchema = StructType(Array(
    StructField("USER_ID", LongType, nullable = false),
    StructField("SUBSCRIBER_ID", LongType, nullable = false),
    StructField("MESSAGE_ID", LongType, nullable = false)
  ))
  val userDirectorySchema = StructType(Array(
    StructField("USER_ID", LongType, nullable = false),
    StructField("FIRST_NAME", LongType, nullable = false),
    StructField("LAST_NAME", LongType, nullable = false),
  ))

  val files = Seq(
    "USER_DIR",
    "MESSAGE_DIR",
    "MESSAGE",
    "RETWEET"
  )

  val networkSchema: Map[String, StructType] = Map(
    "MESSAGE"  -> messageSchema,
    "MESSAGE_DIR"  -> messageDirectorySchema,
    "USER_DIR" -> userDirectorySchema,
    "RETWEET" -> retweetSchema
  )

  /**
   * Counts the number of retweets for each user-message combination.
   *
   * @param df DataFrame containing retweet data
   * @param userCol Name of the user ID column
   * @param messageCol Name of the message ID column
   * @return DataFrame with counts of retweets grouped by user and message
   */
  def countRetweets(df: DataFrame, userCol: String, messageCol: String): DataFrame = {
    df.groupBy(userCol, messageCol).count()
  }

  /**
   * Analyzes the propagation of retweets through a social network in waves.
   * Each wave represents how far a message has traveled from its original source.
   *
   * @param initialUsers DataFrame containing the initial message authors
   * @param retweetDF DataFrame containing retweet relationships
   * @param maxWaves Maximum number of propagation waves to analyze
   * @return DataFrame with retweet counts at each propagation depth
   */
  def retweetWaveFilter(initialUsers: DataFrame, retweetDF: DataFrame, maxWaves: Int): DataFrame = {
    val initialWave = countWave(initialUsers, retweetDF, 0)

    val (finalWaves, resultDF) = (1 to maxWaves).foldLeft((initialUsers, initialWave)) {
      case ((prevWaveUsers, accumulatedDF), depth) =>
        val nextWave = computeNextWave(prevWaveUsers, retweetDF)
        val countedWave = countWave(nextWave, retweetDF, depth)
        (nextWave, accumulatedDF.union(countedWave))
    }

    resultDF.orderBy("MESSAGE_ID", "depth")
  }


  /**
   * Computes the next wave of retweets in the propagation chain.
   * Identifies subscribers who retweeted messages from the previous wave.
   *
   * @param previousWave DataFrame containing users from the previous wave
   * @param retweetDF DataFrame containing retweet relationships
   * @return DataFrame with users in the next propagation wave
   */
  def computeNextWave(previousWave: DataFrame, retweetDF: DataFrame): DataFrame = {
    retweetDF.as("r")
      .join(previousWave.as("w"),
        col("r.USER_ID") === col("w.USER_ID") &&
          col("r.MESSAGE_ID") === col("w.MESSAGE_ID"))
      .select(col("r.SUBSCRIBER_ID").alias("USER_ID"), col("r.MESSAGE_ID"))
  }

  /**
   * Counts retweets for users at a specific propagation depth/wave.
   *
   * @param waveUsers DataFrame containing users at a specific propagation depth
   * @param retweetDF DataFrame containing retweet relationships
   * @param depth The current propagation depth/wave number
   * @return DataFrame with retweet counts for the current wave
   */
  def countWave(waveUsers: DataFrame, retweetDF: DataFrame, depth: Int): DataFrame = {
    val retweetCounts = countRetweets(retweetDF, "USER_ID", "MESSAGE_ID").as("c")
    
    waveUsers.as("w")
      .join(retweetCounts,
        col("w.USER_ID") === col("c.USER_ID") &&
          col("w.MESSAGE_ID") === col("c.MESSAGE_ID"))
      .select(
        col("w.USER_ID"),
        col("w.MESSAGE_ID"),
        col("c.count"),
        lit(depth).alias("depth")
      )
  }
}
