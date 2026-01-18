# IoT ETL PostgreSQL Pipeline

## What this pipeline does

**Bronze**: Raw sensor readings (ingested)

**Silver**: Cleaned + validated readings (deduplicated, range-validated, typed)

**Gold**: Daily KPIs per device & location (count, avg temp/humidity, min battery)

## Data Quality checks

- Null profiling for key columns
- Duplicate row detection
- Range validations:
  - temperature: -20 to 80
  - humidity: 0 to 100
  - battery: 0 to 100

## How to run

```powershell
python src\generate_data.py --rows 50000 --days 14
python src\transform.py
python src\load_and_report.py
```

## Testing

Run smoke tests to validate the pipeline state:

```powershell
python tests_smoke.py
```

Tests verify:
- Output files exist (dq_report.csv, cleaned.csv, daily_kpis.csv)
- Data integrity (required columns, no nulls)
- Data ranges (temperature, humidity, battery valid ranges)
- Database tables populated (bronze, silver, gold layers)

## Outputs

- `outputs/dq_report.csv`
- `outputs/daily_kpis.csv`

## Key SQL

- `sql/01_schema.sql` (tables)
- `sql/02_refresh_gold.sql` (gold refresh)
