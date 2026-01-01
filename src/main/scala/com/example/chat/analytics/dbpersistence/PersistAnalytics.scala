package com.example.chat.analytics.dbpersistence

import org.apache.spark.sql.Row

import java.sql.{Connection, PreparedStatement}

object PersistAnalytics {
  private val jdbcUrl: String = "jdbc:mysql://localhost:3306/chatanalytics" // TODO: Fetch from config
  private val dbProperties: java.util.Properties = {
    val jdbcUser = "root"
    val jdbcPassword = "chatanalytics"
    val props = new java.util.Properties()
    props.setProperty("user", jdbcUser)
    props.setProperty("password", jdbcPassword)
    props.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    props
  }

  def writeChannelMetricsToDB(partitionIter: Iterator[Row]): Unit = {
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

  def writeGlobalLatenciesToDB(partitionIter: Iterator[Row]): Unit = {
    var conn: Connection = null
    var stmt: PreparedStatement = null

    try {
      conn = java.sql.DriverManager.getConnection(jdbcUrl, dbProperties)
      conn.setAutoCommit(false)

      stmt = conn.prepareStatement(
        """
          INSERT INTO global_metrics_minute (
            window_start,
            window_end,
            p50_latency_ms,
            p99_latency_ms
          )
          VALUES (?, ?, ?, ?)
          ON DUPLICATE KEY UPDATE
            window_start = VALUES(window_start),
            window_end = VALUES(window_end),
            p50_latency_ms = VALUES(p50_latency_ms),
            p99_latency_ms = VALUES(p99_latency_ms)
          """
      )

      partitionIter.foreach { (row: Row) =>
        stmt.setTimestamp(1, row.getAs[java.sql.Timestamp]("window_start"))
        stmt.setTimestamp(2, row.getAs[java.sql.Timestamp]("window_end"))
        stmt.setLong(3, row.getAs[Long]("p50_message_delay_ms"))
        stmt.setLong(4, row.getAs[Long]("p99_message_delay_ms"))

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
