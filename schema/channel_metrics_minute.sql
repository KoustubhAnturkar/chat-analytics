CREATE TABLE IF NOT EXISTS channel_metrics_minute (
                                        channel_id        VARCHAR(64) NOT NULL,
                                        window_start      TIMESTAMP NOT NULL,
                                        window_end        TIMESTAMP NOT NULL,

                                        message_count     INT NOT NULL,
                                        active_users      INT NOT NULL,
                                        avg_message_len   FLOAT,

                                        PRIMARY KEY (channel_id, window_start)
);

ALTER TABLE channel_metrics_minute
    ADD INDEX idx_channel_metrics_time (window_start);