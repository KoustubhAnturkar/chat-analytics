CREATE TABLE user_engagement_daily (
  user_id       VARCHAR(64),
  date          DATE,

  messages_sent LONG,
  active_channels LONG,

  PRIMARY KEY (user_id, date)
);
