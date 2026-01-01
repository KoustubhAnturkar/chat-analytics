package com.example.chat

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.protobuf.functions.from_protobuf
import org.apache.spark.sql.functions._
import com.example.chat.analytics.dbpersistence.PersistAnalytics

object QueueStream {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession // TODO: Fetch from config
      .builder
      .appName("ChatAnalyticsStream")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.blockManager.port", "0")
      .config("spark.port.maxRetries", "64")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val protoFileDescriptorPath = if (args.length > 0) {
      args(0)
    } else {
      "../proto/desc/chat_all.desc"
    }

    val kafkaDf = spark.readStream.
          format("kafka").
          option("kafka.bootstrap.servers", "localhost:9092").
          option("subscribe", "chat-stream").
          option("startingOffsets", "earliest").
          option("key.deserializer" , "org.apache.kafka.common.serialization.StringDeserializer").
          option("value.deserializer" , "org.apache.kafka.common.serialization.ByteArrayDeserializer").
          option("max.poll.interval.ms", "5000").
          load()

    val channelMetricsDf = getChannelMetricsDf(kafkaDf, protoFileDescriptorPath)

    val globalLatenciesDf = getGlobalLatenciesDf(kafkaDf, protoFileDescriptorPath)

    val globalUserMetricsDf = getGlobalUserMetricsDf(kafkaDf, protoFileDescriptorPath)

//    globalUserMetricsDf.
//        writeStream
//        .format("console")
//        .option("truncate", "false")
//        .outputMode("update")
//        .start()
//        .awaitTermination()

    val channelMetricsQuery =
      channelMetricsDf
        .writeStream
        .outputMode("update")
        .foreachBatch {
          (batchDf: DataFrame, batchId: Long) =>
          batchDf.foreachPartition { (partitionIter: Iterator[Row]) =>
            PersistAnalytics.writeChannelMetricsToDB(partitionIter)
          }
        }
        .start()

    val globalLatenciesQuery =
      globalLatenciesDf.
        writeStream
        .outputMode("update")
        .foreachBatch{
          (batchDf: DataFrame, batchId: Long) =>
            batchDf.foreachPartition { (partitionIter: Iterator[Row]) =>
              PersistAnalytics.writeGlobalLatenciesToDB(partitionIter)
            }
        }
        .start()

    val userMetricsQuery =
      globalUserMetricsDf.
        writeStream
        .outputMode("update")
        .foreachBatch{
          (batchDf: DataFrame, batchId: Long) =>
            batchDf.foreachPartition { (partitionIter: Iterator[Row]) =>
              PersistAnalytics.writeUserMetricsToDB(partitionIter)
            }
        }
        .start()

    globalLatenciesQuery.awaitTermination()
    channelMetricsQuery.awaitTermination()
    userMetricsQuery.awaitTermination()
  }

  private def getChannelMetricsDf(kafkaDf: DataFrame, protoFileDescriptorPath: String): DataFrame = {
    kafkaDf
      // Convert protobuf binary to structured format
      .select(
        from_protobuf(
          col("value"),
          "ChatMessage",
          protoFileDescriptorPath
        ).as("message"),
        col("timestamp")
      )
      // Extract relevant fields and compute metrics
      .select(
        col("message.channel.channel_id").as("channel_id"),
        col("message.sender.user_id").as("user_id"),
        len(col("message.body")).as("message_length"),
        from_unixtime(
          col("message.time_stamp").divide(1000)
        ).cast("timestamp")
          .as("message_timestamp")
      )
      // Aggregate metrics per channel in 1-minute tumbling windows
      .withWatermark(
        "message_timestamp",
        "2 minutes"
      ).groupBy(
        col("channel_id"),
        window(col("message_timestamp"), "1 minute")
      ).agg(
        count(lit(1)).as("message_count"),
        approx_count_distinct("user_id").as("unique_user_count"),
        round(avg("message_length"), 2).as("avg_message_length"),
      )
      // Final selection of columns
      .select(
        col("channel_id"),
        col("window.start").as("window_start"),
        col("window.end").as("window_end"),
        col("message_count"),
        col("unique_user_count"),
        col("avg_message_length")
      )
  }

  private def getGlobalLatenciesDf(kafkaDf: DataFrame, protoFileDescriptorPath: String): DataFrame = {
    val latenciesDf =  kafkaDf.select(
        from_protobuf(
          col("value"),
          "ChatMessage",
          protoFileDescriptorPath
        ).as("message"),
        col("timestamp")
      ).select(
        from_unixtime(
          col("message.time_stamp").divide(1000)
        ).cast("timestamp")
          .as("message_timestamp"),
        (unix_millis(col("timestamp"))
          .as("ingestion_timestamp") -
          col("message.time_stamp")
            .as("message_timestamp")).as("message_delay_ms")
      ).withWatermark(
        "message_timestamp",
        "2 minutes"
      ).groupBy(
        window(col("message_timestamp"), "1 minute")
      ).agg(
        percentile_approx(col("message_delay_ms"), lit(0.50), lit(1000)).as("p50_message_delay_ms"),
        percentile_approx(col("message_delay_ms"), lit(0.99), lit(1000)).as("p99_message_delay_ms")
      ).select(
        col("window.start").as("window_start"),
        col("window.end").as("window_end"),
        col("p50_message_delay_ms"),
        col("p99_message_delay_ms")
      )
    latenciesDf
  }

  def getGlobalUserMetricsDf(kafkaDf: DataFrame, protoFileDescriptorPath: String): DataFrame = {
    // TODO: Implement global user metrics aggregation on Kafka stream
    kafkaDf
      .select(
        from_protobuf(col("value"), "ChatMessage", protoFileDescriptorPath).as("message"),
        col("timestamp")
      )
      .select(
        col("message.sender.user_id").as("user_id"),
        col("message.channel.channel_id").as("channel_id"),
        from_unixtime(col("message.time_stamp").divide(1000)).cast("timestamp").as("message_timestamp")
      )
      .withWatermark("message_timestamp", "1 day 1 hour")
      .groupBy(
        col("user_id"),
        window(col("message_timestamp"), "1 day", "1 day", "0 seconds")
      )
      .agg(
        count(lit(1)).as("message_count"),
        approx_count_distinct("channel_id").as("unique_channel_count")
      )
      .select(
        col("user_id"),
        col("window.start").as("window_start"),
        col("window.end").as("window_end"),
        col("message_count"),
        col("unique_channel_count")
      )
  }
}
