"""
Smoke tests for IoT ETL Pipeline
Tests basic functionality, file integrity, and database state
"""

import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import psycopg2

load_dotenv()

def assert_true(cond, msg):
    if not cond:
        raise AssertionError(msg)

def main():
    # 1) File checks
    dq_path = Path("outputs/dq_report.csv")
    cleaned_path = Path("data/processed/cleaned.csv")
    kpi_path = Path("outputs/daily_kpis.csv")

    assert_true(dq_path.exists(), "Missing outputs/dq_report.csv")
    assert_true(cleaned_path.exists(), "Missing data/processed/cleaned.csv")
    assert_true(kpi_path.exists(), "Missing outputs/daily_kpis.csv")

    # 2) Cleaned data checks
    df = pd.read_csv(cleaned_path)
    required = ["event_time","device_id","temperature_c","humidity_pct","battery_pct","location","event_date"]
    for c in required:
        assert_true(c in df.columns, f"Missing column in cleaned.csv: {c}")
        assert_true(df[c].isna().sum() == 0, f"Nulls found in cleaned.csv column: {c}")

    assert_true(((df["temperature_c"] >= -20) & (df["temperature_c"] <= 80)).all(), "Temperature out of range in cleaned.csv")
    assert_true(((df["humidity_pct"] >= 0) & (df["humidity_pct"] <= 100)).all(), "Humidity out of range in cleaned.csv")
    assert_true(((df["battery_pct"] >= 0) & (df["battery_pct"] <= 100)).all(), "Battery out of range in cleaned.csv")

    # 3) Postgres checks
    conn = psycopg2.connect(
        host=os.getenv("PGHOST"),
        port=os.getenv("PGPORT"),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
    )
    cur = conn.cursor()

    for table in ["bronze_sensor_readings","silver_sensor_readings","gold_daily_device_kpis"]:
        cur.execute(f"SELECT COUNT(*) FROM {table};")
        count = cur.fetchone()[0]
        assert_true(count > 0, f"Table empty: {table}")

    cur.close()
    conn.close()

    print("ALL TESTS PASSED")

if __name__ == "__main__":
    main()
