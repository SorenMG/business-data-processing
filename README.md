# When Should Taxi Drivers Work?

Revenue Patterns Across Time and Space in NYC Yellow Taxi Data
**Business Data Processing & Business Intelligence**

The project builds an ETL pipeline in **Python**, stores curated data in **PostgreSQL**, and prepares exports for **Tableau** dashboards.

## Tech Stack

* **Python** (pandas, SQLAlchemy)
* **PostgreSQL** (analytical schema)
* **Tableau Desktop** (visual analytics)

## Data Sources

The data is gathered from the TLC New York data set: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

* **NYC TLC Yellow Taxi Trip Records** (monthly Parquet files)
* **Taxi Zone Lookup** (zone/borough metadata)

This project uses the Jan–Mar 2023.

---

## Setup

### 1) Create a Python environment

```bash
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .venv\Scripts\activate   # Windows
pip install -r requirements.txt
```

### 2) Ensure PostgreSQL is running

Create an empty database, e.g.:

```sql
CREATE DATABASE nyc_taxi;
```

### 3) Run pipeline

```
python3 pipeline.py
```

---

## License / Usage

This repository is intended for coursework/exam submission and learning purposes. If you reuse or extend it, cite NYC TLC as the data provider and document any modifications to the pipeline or definitions.

