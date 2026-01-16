TRUNCATE TABLE gold_daily_device_kpis;

INSERT INTO gold_daily_device_kpis (event_date, device_id, location, reading_count, avg_temp_c, avg_humidity, min_battery)
SELECT
  event_date,
  device_id,
  location,
  COUNT(*) AS reading_count,
  ROUND(AVG(temperature_c), 2) AS avg_temp_c,
  ROUND(AVG(humidity_pct), 2) AS avg_humidity,
  MIN(battery_pct) AS min_battery
FROM silver_sensor_readings
GROUP BY event_date, device_id, location;
