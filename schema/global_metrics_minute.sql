CREATE TABLE global_metrics_minute (
                                       window_start      TIMESTAMP PRIMARY KEY,

                                       total_messages    INT NOT NULL,
                                       active_channels   INT NOT NULL,
                                       active_users      INT NOT NULL,

                                       p50_latency_ms    INT,
                                       p95_latency_ms    INT
);
