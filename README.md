# When Should Taxi Drivers Work?

Revenue Patterns Across Time and Space in NYC Yellow Taxi Data
**Business Data Processing & Business Intelligence — Exam Project**

## Overview

This repository contains an end-to-end BI/analytics pipeline that answers:

* **When should a NYC yellow taxi driver work to maximize revenue?**
* **Where should they work to maximize revenue?**

The project builds an ETL pipeline in **Python**, stores curated data in **PostgreSQL** (star schema + views), and prepares exports for **Tableau** dashboards.

## Tech Stack

* **Python** (pandas, SQLAlchemy, pyarrow)
* **PostgreSQL** (analytical schema + SQL views)
* **Tableau Desktop** (visual analytics)

## Data Sources (public)

* **NYC TLC Yellow Taxi Trip Records** (monthly Parquet files)
* **Taxi Zone Lookup** (CSV mapping `LocationID` → zone/borough metadata)

This project uses a **Jan–Mar 2023** sample to keep processing manageable while preserving meaningful patterns.

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

### 3) Configure DB connection

Set your connection string in your Python pipeline script (SQLAlchemy), for example:

```
postgresql+psycopg2://user:password@localhost:5432/nyc_taxi
```

---

## License / Usage

This repository is intended for coursework/exam submission and learning purposes. If you reuse or extend it, cite NYC TLC as the data provider and document any modifications to the pipeline or definitions.

