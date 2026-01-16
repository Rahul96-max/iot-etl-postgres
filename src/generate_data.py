import argparse, json, random
from pathlib import Path
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

def generate(rows: int, days: int, devices: int = 50) -> pd.DataFrame:
    now = datetime.now()
    start = now - timedelta(days=days)

    device_ids = [f"dev-{i:03d}" for i in range(1, devices + 1)]
    locations = ["Bangalore", "Pune", "Hyderabad", "Chennai", "Mumbai"]

    times = [start + timedelta(seconds=random.randint(0, days * 24 * 3600)) for _ in range(rows)]
    times.sort()

    df = pd.DataFrame({
        "event_time": times,
        "device_id": np.random.choice(device_ids, size=rows),
        "temperature_c": np.random.normal(loc=28, scale=8, size=rows).round(2),
        "humidity_pct": np.random.normal(loc=55, scale=15, size=rows).round(2),
        "battery_pct": np.random.normal(loc=70, scale=20, size=rows).round(2),
        "location": np.random.choice(locations, size=rows)
    })

    # bad data for DQ demo
    bad_idx = np.random.choice(df.index, size=max(1, rows // 100), replace=False)
    df.loc[bad_idx, "temperature_c"] = np.random.choice([120, -50, None], size=len(bad_idx))
    bad_idx2 = np.random.choice(df.index, size=max(1, rows // 120), replace=False)
    df.loc[bad_idx2, "humidity_pct"] = np.random.choice([150, -10, None], size=len(bad_idx2))
    bad_idx3 = np.random.choice(df.index, size=max(1, rows // 150), replace=False)
    df.loc[bad_idx3, "battery_pct"] = np.random.choice([200, -5, None], size=len(bad_idx3))

    # duplicates
    if rows >= 10:
        dup_rows = df.sample(n=min(200, max(10, rows // 50))).copy()
        df = pd.concat([df, dup_rows], ignore_index=True)

    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=50000)
    ap.add_argument("--days", type=int, default=14)
    args = ap.parse_args()

    out_dir = Path("data/raw")
    out_dir.mkdir(parents=True, exist_ok=True)

    df = generate(args.rows, args.days)
    df.to_csv(out_dir / "sensor_readings.csv", index=False)

    with open(out_dir / "sensor_readings.jsonl", "w", encoding="utf-8") as f:
        for _, row in df.iterrows():
            rec = row.to_dict()
            rec["event_time"] = pd.to_datetime(rec["event_time"]).isoformat()
            f.write(json.dumps(rec) + "\n")

    print("Generated:", len(df))
    print("Created: data/raw/sensor_readings.csv and data/raw/sensor_readings.jsonl")

if __name__ == "__main__":
    main()
