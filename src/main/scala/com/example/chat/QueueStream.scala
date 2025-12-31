package com.example.chat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.protobuf.functions.from_protobuf
import org.apache.spark.sql.functions._

object QueueStream {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("ChatAnalyticsStream")
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

    val channelMetricsDf =
      kafkaDf
        .select(
          from_protobuf(
            col("value"),
            "ChatMessage",
            protoFileDescriptorPath
          ).as("message")
        )
        .select(
          col("message.channel.channel_id").as("channel_id"),
          col("message.sender.user_id").as("user_id"),
          from_unixtime(col("message.time_stamp").divide(
            1000
          )).as("timestamp")
        ).groupBy(col("channel_id"), window(col("timestamp"), "1 minute")).sum()

    channelMetricsDf.writeStream
      .format("console")
      .option("truncate", "false")
      .start()
      .awaitTermination()

  }
}
