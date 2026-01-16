from pathlib import Path
import pandas as pd

RANGES = {
    "temperature_c": (-20, 80),
    "humidity_pct": (0, 100),
    "battery_pct": (0, 100),
}

def dq_report(df: pd.DataFrame) -> pd.DataFrame:
    rows = len(df)
    rep = []
    for col in ["event_time","device_id","temperature_c","humidity_pct","battery_pct","location"]:
        rep.append({
            "check":"null_count",
            "column":col,
            "value":int(df[col].isna().sum()),
            "pct": float(df[col].isna().mean()*100) if rows else 0.0
        })

    dup_count = int(df.duplicated().sum())
    rep.append({"check":"duplicate_rows","column":"*","value":dup_count,"pct": (dup_count/rows*100) if rows else 0.0})

    for col,(lo,hi) in RANGES.items():
        bad = df[col].isna() | (df[col] < lo) | (df[col] > hi)
        rep.append({
            "check":"out_of_range",
            "column":col,
            "value":int(bad.sum()),
            "pct": float(bad.mean()*100) if rows else 0.0
        })

    return pd.DataFrame(rep)

def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["event_time"] = pd.to_datetime(out["event_time"], errors="coerce")
    out = out.dropna(subset=["event_time","device_id"]).drop_duplicates()

    for col in ["temperature_c","humidity_pct","battery_pct"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out.loc[(out["temperature_c"] < -20) | (out["temperature_c"] > 80), "temperature_c"] = pd.NA
    out.loc[(out["humidity_pct"] < 0) | (out["humidity_pct"] > 100), "humidity_pct"] = pd.NA
    out.loc[(out["battery_pct"] < 0) | (out["battery_pct"] > 100), "battery_pct"] = pd.NA

    out = out.dropna(subset=["temperature_c","humidity_pct","battery_pct","location"])
    out["event_date"] = out["event_time"].dt.date
    return out

def main():
    raw_csv = Path("data/raw/sensor_readings.csv")
    if not raw_csv.exists():
        raise FileNotFoundError("Run: python src\\generate_data.py")

    df = pd.read_csv(raw_csv)

    Path("outputs").mkdir(parents=True, exist_ok=True)
    dq_report(df).to_csv("outputs/dq_report.csv", index=False)

    cleaned = clean_df(df)
    Path("data/processed").mkdir(parents=True, exist_ok=True)
    cleaned.to_csv("data/processed/cleaned.csv", index=False)

    print("Saved outputs/dq_report.csv and data/processed/cleaned.csv")
    print("Cleaned rows:", len(cleaned))

if __name__ == "__main__":
    main()
