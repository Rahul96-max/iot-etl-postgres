CREATE TABLE IF NOT EXISTS bronze_sensor_readings (
  event_time    TIMESTAMP NOT NULL,
  device_id     TEXT NOT NULL,
  temperature_c NUMERIC,
  humidity_pct  NUMERIC,
  battery_pct   NUMERIC,
  location      TEXT
);

CREATE TABLE IF NOT EXISTS silver_sensor_readings (
  event_time    TIMESTAMP NOT NULL,
  event_date    DATE NOT NULL,
  device_id     TEXT NOT NULL,
  temperature_c NUMERIC NOT NULL,
  humidity_pct  NUMERIC NOT NULL,
  battery_pct   NUMERIC NOT NULL,
  location      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gold_daily_device_kpis (
  event_date    DATE NOT NULL,
  device_id     TEXT NOT NULL,
  location      TEXT NOT NULL,
  reading_count INT NOT NULL,
  avg_temp_c    NUMERIC NOT NULL,
  avg_humidity  NUMERIC NOT NULL,
  min_battery   NUMERIC NOT NULL,
  PRIMARY KEY (event_date, device_id, location)
);
