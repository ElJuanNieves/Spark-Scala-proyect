package com.social_network

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, StructField}
import org.apache.spark.sql.types.*
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object SocialNetwortUtils {

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
    df
      .groupBy(user, message)
      .count()
  }

  def retweet_wave_filter(df : DataFrame): DataFrame = {

    val base = df
      .select("USER_ID", "MESSAGE_ID")
      .distinct()
      .withColumnRenamed("USER_ID", "CURRENT_USER")
      .withColumn("depth", lit(0))
    
    base.show()

    val wave1 = expandWave(base, df, 1)
    val wave2 = expandWave(wave1, df, 2)

    val allWaves = base.union(wave1).union(wave2)
      .dropDuplicates("CURRENT_USER", "MESSAGE_ID")
      .withColumnRenamed("CURRENT_USER", "USER_ID")
    
    val filterWaves = allWaves.filter(col("depth") > 0 && col("depth") <= 2)
    filterWaves.show(40, truncate = true)
    allWaves
      .filter(col("depth") > 0 && col("depth") <= 2)
  }

  def expandWave(df: DataFrame, df_retweet: DataFrame, depth: Int): DataFrame = {
    val a = df.as("a")
    val b = df_retweet.as("b")

    a.join(b,
        col("a.CURRENT_USER") === col("b.USER_ID") &&
          col("a.MESSAGE_ID") === col("b.MESSAGE_ID")
      )
      .select(
        col("b.SUBSCRIBER_ID").as("CURRENT_USER"),
        col("b.MESSAGE_ID")
      )
      .withColumn("depth", lit(depth))
  }
}
