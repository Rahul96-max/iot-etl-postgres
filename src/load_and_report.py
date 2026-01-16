from pathlib import Path
import pandas as pd
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.db import get_pg_conn

def run_sql(cur, path: Path):
    cur.execute(path.read_text(encoding="utf-8"))

def main():
    load_dotenv()

    cleaned = Path("data/processed/cleaned.csv")
    if not cleaned.exists():
        raise FileNotFoundError("Run: python src\\transform.py")

    df = pd.read_csv(cleaned)
    df["event_time"] = pd.to_datetime(df["event_time"])
    df["event_date"] = pd.to_datetime(df["event_date"]).dt.date

    conn = get_pg_conn()
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            run_sql(cur, Path("sql/01_schema.sql"))

            cur.execute("TRUNCATE TABLE bronze_sensor_readings;")
            bronze_cols = ["event_time","device_id","temperature_c","humidity_pct","battery_pct","location"]
            execute_values(
                cur,
                "INSERT INTO bronze_sensor_readings (event_time, device_id, temperature_c, humidity_pct, battery_pct, location) VALUES %s",
                [tuple(x) for x in df[bronze_cols].to_numpy()],
                page_size=5000
            )

            cur.execute("TRUNCATE TABLE silver_sensor_readings;")
            silver_cols = ["event_time","event_date","device_id","temperature_c","humidity_pct","battery_pct","location"]
            execute_values(
                cur,
                "INSERT INTO silver_sensor_readings (event_time, event_date, device_id, temperature_c, humidity_pct, battery_pct, location) VALUES %s",
                [tuple(x) for x in df[silver_cols].to_numpy()],
                page_size=5000
            )

            run_sql(cur, Path("sql/02_refresh_gold.sql"))
            conn.commit()

        kpi = pd.read_sql_query(
            "SELECT * FROM gold_daily_device_kpis ORDER BY event_date DESC, device_id LIMIT 5000;",
            conn
        )
        Path("outputs").mkdir(parents=True, exist_ok=True)
        kpi.to_csv("outputs/daily_kpis.csv", index=False)
        print("Exported outputs/daily_kpis.csv")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
