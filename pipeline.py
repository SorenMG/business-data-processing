from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://user:password@localhost:5432/nyc_taxi"

MONTHS = [
    "2023-01",
    "2023-02",
    "2023-03",
]

YELLOW_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"

OUTPUT_DIR = "output"


def clear_all_data(engine):
    """Delete all existing data from the database before reloading"""
    print("Clearing existing data...")
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS trip CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS time_dim CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS zone CASCADE;"))
    print("All data cleared.")


def run_schema(engine):
    """Execute schema.sql to (re)create tables"""
    with open("schema.sql", "r", encoding="utf-8") as f:
        schema_sql = f.read()
    with engine.begin() as conn:
        conn.execute(text(schema_sql))


def load_zones(engine):
    """Download and load taxi zone lookup into zone table"""
    print("Downloading and loading zone lookup...")
    zones = pd.read_csv(ZONE_LOOKUP_URL)

    # make sure we match the actual TLC column names
    zones = zones.rename(
        columns={
            "LocationID": "zone_id",
            "Borough": "borough",
            "Zone": "zone_name",
            "service_zone": "service_zone",
        }
    )

    zones["is_airport"] = zones["zone_name"].str.contains(
        "Airport", case=False, na=False
    )
    zones["is_manhattan"] = zones["borough"].str.upper().eq("MANHATTAN")

    with engine.begin() as conn:
        zones.to_sql("zone", conn, if_exists="append", index=False)

    print(f"Loaded {len(zones)} zones.")


def upsert_time_dim_from_df(trips: pd.DataFrame, engine):
    """Insert unique pickup_date rows into time_dim, ignoring duplicates"""

    # extract pickup_date
    trips["pickup_date"] = trips["pickup_datetime"].dt.date
    time_dim = trips[["pickup_date"]].drop_duplicates().copy()

    # derive attributes
    idx = pd.to_datetime(time_dim["pickup_date"])

    time_dim["pickup_year"] = idx.dt.year
    time_dim["pickup_month"] = idx.dt.month
    time_dim["pickup_day"] = idx.dt.day
    time_dim["pickup_week"] = idx.dt.isocalendar().week.astype(int)
    time_dim["pickup_weekday"] = idx.dt.weekday
    time_dim["is_weekend"] = time_dim["pickup_weekday"].isin([5, 6])

    records = time_dim.to_dict(orient="records")
    if not records:
        return

    stmt = text("""
        INSERT INTO time_dim (
            pickup_date,
            pickup_year,
            pickup_month,
            pickup_day,
            pickup_week,
            pickup_weekday,
            is_weekend
        )
        VALUES (
            :pickup_date,
            :pickup_year,
            :pickup_month,
            :pickup_day,
            :pickup_week,
            :pickup_weekday,
            :is_weekend
        )
        ON CONFLICT (pickup_date) DO NOTHING;
    """)

    with engine.begin() as conn:
        conn.execute(stmt, records)


def get_time_dim_mapping(engine):
    """Return mapping from pickup_date to date_id"""
    df = pd.read_sql("SELECT date_id, pickup_date FROM time_dim", engine)
    df["pickup_date"] = pd.to_datetime(df["pickup_date"]).dt.date
    return df.set_index("pickup_date")["date_id"]


def load_trips(engine):
    """Download and load yellow taxi trips for each month"""
    for month in MONTHS:
        print(f"Processing month {month}...")
        url = f"{YELLOW_BASE_URL}/yellow_tripdata_{month}.parquet"

        cols = [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "PULocationID",
            "DOLocationID",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "total_amount",
            "payment_type",
        ]

        trips = pd.read_parquet(url, columns=cols)

        # basic cleaning
        trips = trips.dropna(
            subset=["tpep_pickup_datetime",
                    "tpep_dropoff_datetime", "total_amount"]
        )
        trips = trips[trips["trip_distance"] > 0]
        trips = trips[trips["total_amount"] > 0]

        trips["tpep_pickup_datetime"] = pd.to_datetime(
            trips["tpep_pickup_datetime"]
        )
        trips["tpep_dropoff_datetime"] = pd.to_datetime(
            trips["tpep_dropoff_datetime"]
        )

        trips["trip_duration_min"] = (
            (trips["tpep_dropoff_datetime"] - trips["tpep_pickup_datetime"])
            .dt.total_seconds()
            / 60.0
        )
        trips = trips[trips["trip_duration_min"] > 0]

        upsert_time_dim_from_df(
            trips.rename(columns={"tpep_pickup_datetime": "pickup_datetime"}),
            engine,
        )

        time_map = get_time_dim_mapping(engine)
        pickup_dates = trips["tpep_pickup_datetime"].dt.date
        trips["date_id"] = pickup_dates.map(time_map)

        trips = trips.rename(
            columns={
                "tpep_pickup_datetime": "pickup_datetime",
                "tpep_dropoff_datetime": "dropoff_datetime",
                "PULocationID": "pu_zone_id",
                "DOLocationID": "do_zone_id",
                "trip_distance": "trip_distance_mi",
            }
        )

        trip_cols = [
            "pickup_datetime",
            "dropoff_datetime",
            "date_id",
            "pu_zone_id",
            "do_zone_id",
            "passenger_count",
            "trip_distance_mi",
            "trip_duration_min",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "total_amount",
            "payment_type",
        ]

        with engine.begin() as conn:
            trips[trip_cols].to_sql(
                "trip", conn, if_exists="append", index=False
            )

        print(f"Inserted {len(trips)} trips for {month}.")


def run_views(engine):
    """Execute views.sql to create analysis views"""
    with open("views.sql", "r", encoding="utf-8") as f:
        views_sql = f.read()
    with engine.begin() as conn:
        conn.execute(text(views_sql))


def export_for_tableau(engine):
    """Export key views to CSV for Tableau"""
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    views = [
        "trip_enriched",
        "zone_hourly_revenue",
    ]

    for v in views:
        print(f"Exporting {v} ...")
        df = pd.read_sql(f"SELECT * FROM {v}", engine)
        out_path = Path(OUTPUT_DIR) / f"{v}.csv"
        df.to_csv(out_path, index=False)
        print(f"Saved {out_path}")


def main():
    engine = create_engine(DB_URL)

    clear_all_data(engine)

    print("1) Creating tables...")
    run_schema(engine)

    print("2) Loading zone dimension...")
    load_zones(engine)

    print("3) Loading trip data...")
    load_trips(engine)

    print("4) Creating views...")
    run_views(engine)

    print("5) Exporting CSVs for Tableau...")
    export_for_tableau(engine)

    print("Done.")


if __name__ == "__main__":
    main()
