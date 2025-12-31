package com.example.chat

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.protobuf.functions.from_protobuf
import org.apache.spark.sql.functions._

import java.sql.{Connection, PreparedStatement}

object QueueStream {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession // TODO: Fetch from config
      .builder
      .appName("ChatAnalyticsStream")
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

    val channelMetricsDf = {
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
          len(col("message.body")).as("message_length"),
          from_unixtime(
            col("message.time_stamp").divide(1000)
          ).cast("timestamp")
            .as("timestamp")
        ).withWatermark(
          "timestamp",
          "2 minutes"
        ).groupBy(
          col("channel_id"),
          window(col("timestamp"), "1 minute")
        ).agg(
          count(lit(1)).as("message_count"),
          approx_count_distinct("user_id").as("unique_user_count"),
          round(avg("message_length"), 2).as("avg_message_length")
        ).select(
          col("channel_id"),
          col("window.start").as("window_start"),
          col("window.end").as("window_end"),
          col("message_count"),
          col("unique_user_count"),
          col("avg_message_length")
        )
    }

//         channelMetricsDf
//          .writeStream
//          .format("console")
//          .option("truncate", "false")
//          .outputMode("update")
//          .start()
//          .awaitTermination()

    val jdbcUrl = "jdbc:mysql://localhost:3306/chatanalytics" // TODO: Fetch from config
    val jdbcUser = "root"
    val jdbcPassword = "chatanalytics"

    val dbProperties = new java.util.Properties()
    dbProperties.setProperty("user", jdbcUser)
    dbProperties.setProperty("password", jdbcPassword)
    dbProperties.setProperty("driver", "com.mysql.cj.jdbc.Driver")

    channelMetricsDf
      .writeStream
      .outputMode("update")
      .foreachBatch { (batchDf: DataFrame, batchId: Long) =>

        batchDf.foreachPartition { (partitionIter: Iterator[Row]) =>
          var conn: Connection = null
          var stmt: PreparedStatement = null

          try {
            conn = java.sql.DriverManager.getConnection(jdbcUrl, dbProperties)
            conn.setAutoCommit(false)

            stmt = conn.prepareStatement(
              """
          INSERT INTO channel_metrics_minute (
            channel_id,
            window_start,
            window_end,
            message_count,
            active_users,
            avg_message_len
          )
          VALUES (?, ?, ?, ?, ?, ?)
          ON DUPLICATE KEY UPDATE
            window_end = VALUES(window_end),
            message_count = VALUES(message_count),
            active_users = VALUES(active_users),
            avg_message_len = VALUES(avg_message_len)
          """
            )

            partitionIter.foreach { (row: Row) =>
              stmt.setString(1, row.getAs[String]("channel_id"))
              stmt.setTimestamp(2, row.getAs[java.sql.Timestamp]("window_start"))
              stmt.setTimestamp(3, row.getAs[java.sql.Timestamp]("window_end"))
              stmt.setInt(4, row.getAs[Long]("message_count").toInt)
              stmt.setInt(5, row.getAs[Long]("unique_user_count").toInt)
              stmt.setDouble(6, row.getAs[Double]("avg_message_length"))

              stmt.addBatch()
            }

            stmt.executeBatch()
            conn.commit()

          } finally {
            if (stmt != null) stmt.close()
            if (conn != null) conn.close()
          }
        }
      }
      .start()
      .awaitTermination()
  }
}
