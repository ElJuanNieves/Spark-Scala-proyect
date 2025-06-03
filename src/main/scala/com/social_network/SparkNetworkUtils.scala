package com.social_network

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, StructField}
import org.apache.spark.sql.types.*
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object SparkNetworkUtils {

  val messageSchema = StructType(Array(
    StructField("USER_ID", LongType, nullable = false),
    StructField("MESSAGE_ID", LongType, nullable = false)
  ))
  val message_dirScehma = StructType(Array(
    StructField("MESSAGE_ID", LongType, nullable = false),
    StructField("TEXT", StringType, nullable = false)
  ))
  val retweetScehma = StructType(Array(
    StructField("USER_ID", LongType, nullable = false),
    StructField("SUBSCRIBER_ID", LongType, nullable = false),
    StructField("MESSAGE_ID", LongType, nullable = false)
  ))
  val user_dirSchema = StructType(Array(
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
    "MESSAGE_DIR"  -> message_dirScehma,
    "USER_DIR" -> user_dirSchema,
    "RETWEET" -> retweetScehma
  )


  def count_retweets(df: DataFrame, user: String, message: String): DataFrame= {
    val df_count = df
      .groupBy(user, message)
      .count()
    df_count
  }

  def retweet_wave_filter(df_user_dir : DataFrame, df_retweet: DataFrame, waves: Int): DataFrame = {
    val wave0 = countWave(df_user_dir, df_retweet, 0)
    wave0
      .orderBy("MESSAGE_ID")
      .show()
    val wave1 = countWave(nextWave(wave0, df_retweet), df_retweet, 1)
    wave1
  }
  def nextWave(df_prev: DataFrame, df_retweet: DataFrame): DataFrame = {

    val df_next = df_retweet
      .as("a")
      .join(df_prev.as("b"), 
        col("a.USER_ID") === col("b.SUBSCRIBER_ID") && 
        col("a.MESSAGE_ID") === col("b.MESSAGE_ID"))
      .select(col("b.SUBSCRIBER_ID"), col("b.MESSAGE_ID"))
      .filter(col("b.SUBSCRIBER_ID").isNotNull)
      .withColumnRenamed("SUBSCRIBER_ID", "USER_ID")
    
    df_next.show()
    df_next
  }

  def countWave(df_user_dir: DataFrame, df_retweet: DataFrame, depth: Int): DataFrame = {
    val count_df = count_retweets(df_retweet, "USER_ID", "MESSAGE_ID").as("b")
    count_df
      .orderBy("MESSAGE_ID")
      .show()
    val discard_waves = df_user_dir
      .as("a")
      .join(count_df,
        col("a.USER_ID") === col("b.USER_ID") && col("a.MESSAGE_ID") === col("b.MESSAGE_ID"))
      .select(col("a.USER_ID"), col("a.MESSAGE_ID"), col("b.count"))
      .withColumn("depth", lit(depth))

    discard_waves
  }
}
