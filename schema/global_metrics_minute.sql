CREATE TABLE global_metrics_minute (
                                       window_start      TIMESTAMP PRIMARY KEY,
                                       window_end        TIMESTAMP NOT NULL,

                                       p50_latency_ms    LONG NOT NULL ,
                                       p99_latency_ms    LONG NOT NULL
);
