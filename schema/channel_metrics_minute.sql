CREATE TABLE IF NOT EXISTS channel_metrics_minute (
                                        channel_id        VARCHAR(64) NOT NULL,
                                        window_start      TIMESTAMP NOT NULL,
                                        window_end        TIMESTAMP NOT NULL,

                                        message_count     INT NOT NULL,
                                        active_users      INT NOT NULL,
                                        avg_message_len   FLOAT,

                                        PRIMARY KEY (channel_id, window_start)
);

CREATE INDEX IF NOT EXISTS idx_channel_metrics_time
    ON channel_metrics_minute (window_start);
