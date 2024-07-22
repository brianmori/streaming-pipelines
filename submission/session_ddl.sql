CREATE TABLE IF NOT EXISTS <their_username>.dataexpert_sessions (
  host STRING,
  session_id STRING,
  user_id BIGINT,
  is_logged_in BOOLEAN,
  country STRING,
  state STRING,
  city STRING,
  browser_family STRING,
  device_family STRING,
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  event_count BIGINT,
  session_date DATE
)
USING ICEBERG
PARTITIONED BY (session_date)