# IoT Sensor Data ETL Pipeline (PostgreSQL)

End-to-end ETL pipeline that generates synthetic IoT sensor data, applies data-quality (DQ) validation and cleaning, loads curated data into PostgreSQL, and exports daily KPI reports for analytics.

This project is structured to resemble a practical data engineering workflow with clear separation of:
- **Ingestion (raw data generation)**
- **Transformation (DQ checks + cleaning)**
- **Load + Reporting (PostgreSQL + KPI export)**
- **Testing (smoke tests for pipeline reliability)**

---

## Features
- Generate synthetic IoT sensor readings at scale (CSV + JSONL)
- Inject realistic bad data (nulls, out-of-range values) to test DQ logic
- Data-quality checks and transformation to produce:
  - DQ report: `outputs/dq_report.csv`
  - Cleaned dataset: `data/processed/cleaned.csv`
- Load cleaned data into **PostgreSQL**
- Create daily KPIs using SQL and export report:
  - `outputs/daily_kpis.csv`
- Run smoke tests to validate end-to-end execution:
  - `tests_smoke.py` → **ALL TESTS PASSED**

---

## Tech Stack
- **Python**, **Pandas**, **NumPy**
- **PostgreSQL**
- (Optional enhancement) **SQLAlchemy** for database connectivity

---

## Project Structure
```text
iot-etl-postgres/
├─ data/
│  ├─ raw/                 # generated raw datasets
│  └─ processed/            # cleaned datasets
├─ outputs/                 # DQ and KPI exports
├─ src/
│  ├─ generate_data.py      # generates synthetic sensor readings (CSV/JSONL)
│  ├─ transform.py          # DQ checks + cleaning
│  └─ load_and_report.py    # loads to Postgres + exports KPIs
├─ tests_smoke.py           # smoke tests for pipeline
└─ README.md
```

---

## Data Flow (Bronze / Silver / Gold)
- **Bronze (Raw):** `data/raw/sensor_readings.csv`, `data/raw/sensor_readings.jsonl`
- **Silver (Cleaned):** `data/processed/cleaned.csv` + DQ findings in `outputs/dq_report.csv`
- **Gold (Reporting):** `outputs/daily_kpis.csv`

---

## Setup

### 1) Create and activate a virtual environment (recommended)

**Windows (PowerShell):**
```bash
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

**macOS/Linux:**
```bash
python -m venv .venv
source .venv/bin/activate
```

### 2) Install dependencies
```bash
pip install -r requirements.txt
```

If you don't have `requirements.txt` yet, generate it after installing packages:
```bash
pip freeze > requirements.txt
```

### 3) Configure PostgreSQL
Create a database and ensure your connection values used inside `src/load_and_report.py` are correct:
- host
- port
- dbname
- user
- password

---

## How to Run (End-to-End)

### Step 1 — Generate raw data
Generate 50,000 rows across 14 days (example used in testing):
```bash
python src/generate_data.py --rows 50000 --days 14
```

Expected outputs:
- `data/raw/sensor_readings.csv`
- `data/raw/sensor_readings.jsonl`

### Step 2 — Transform + DQ validation
```bash
python src/transform.py
```

Expected outputs:
- `outputs/dq_report.csv`
- `data/processed/cleaned.csv`

Console output includes cleaned row count.

### Step 3 — Load to PostgreSQL + KPI report
```bash
python src/load_and_report.py
```

Expected output:
- `outputs/daily_kpis.csv`

---

## Run Tests (Smoke Tests)
```bash
python tests_smoke.py
```

Expected:
- **ALL TESTS PASSED**

---

## Output Artifacts

### Raw
- `data/raw/sensor_readings.csv`
- `data/raw/sensor_readings.jsonl`

### DQ + Cleaned
- `outputs/dq_report.csv`
- `data/processed/cleaned.csv`

### Reporting
- `outputs/daily_kpis.csv`

---

## Author
**Rahul Santosh Jagtap**  
GitHub: https://github.com/Rahul96-max  
LinkedIn: linkedin.com/in/rahul-jagtap-28833324a
