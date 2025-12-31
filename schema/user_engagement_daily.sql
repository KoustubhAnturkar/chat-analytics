CREATE TABLE user_engagement_daily (
  user_id       VARCHAR(64),
  date          DATE,

  messages_sent INT,
  active_channels INT,

  PRIMARY KEY (user_id, date)
);
