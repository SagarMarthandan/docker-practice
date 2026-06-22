"""Column and schema definitions for NYC taxi data (yellow & green)."""

import os

YELLOW_COLUMNS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag",
    "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra",
    "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
    "total_amount", "congestion_surcharge",
]

GREEN_COLUMNS = [
    "VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime",
    "store_and_fwd_flag", "RatecodeID", "PULocationID", "DOLocationID",
    "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax",
    "tip_amount", "tolls_amount", "ehail_fee", "improvement_surcharge",
    "total_amount", "payment_type", "trip_type", "congestion_surcharge",
]

YELLOW_DEDUP_COLS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "PULocationID", "DOLocationID", "fare_amount", "trip_distance",
]

GREEN_DEDUP_COLS = [
    "VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime",
    "PULocationID", "DOLocationID", "fare_amount", "trip_distance",
]

YELLOW_BQ_SCHEMA = [
    {"name": "VendorID", "type": "STRING"},
    {"name": "tpep_pickup_datetime", "type": "TIMESTAMP"},
    {"name": "tpep_dropoff_datetime", "type": "TIMESTAMP"},
    {"name": "passenger_count", "type": "FLOAT64"},
    {"name": "trip_distance", "type": "FLOAT64"},
    {"name": "RatecodeID", "type": "STRING"},
    {"name": "store_and_fwd_flag", "type": "STRING"},
    {"name": "PULocationID", "type": "INTEGER"},
    {"name": "DOLocationID", "type": "INTEGER"},
    {"name": "payment_type", "type": "INTEGER"},
    {"name": "fare_amount", "type": "FLOAT64"},
    {"name": "extra", "type": "FLOAT64"},
    {"name": "mta_tax", "type": "FLOAT64"},
    {"name": "tip_amount", "type": "FLOAT64"},
    {"name": "tolls_amount", "type": "FLOAT64"},
    {"name": "improvement_surcharge", "type": "FLOAT64"},
    {"name": "total_amount", "type": "FLOAT64"},
    {"name": "congestion_surcharge", "type": "FLOAT64"},
]

GREEN_BQ_SCHEMA = [
    {"name": "VendorID", "type": "STRING"},
    {"name": "lpep_pickup_datetime", "type": "TIMESTAMP"},
    {"name": "lpep_dropoff_datetime", "type": "TIMESTAMP"},
    {"name": "store_and_fwd_flag", "type": "STRING"},
    {"name": "RatecodeID", "type": "STRING"},
    {"name": "PULocationID", "type": "INTEGER"},
    {"name": "DOLocationID", "type": "INTEGER"},
    {"name": "passenger_count", "type": "FLOAT64"},
    {"name": "trip_distance", "type": "FLOAT64"},
    {"name": "fare_amount", "type": "FLOAT64"},
    {"name": "extra", "type": "FLOAT64"},
    {"name": "mta_tax", "type": "FLOAT64"},
    {"name": "tip_amount", "type": "FLOAT64"},
    {"name": "tolls_amount", "type": "FLOAT64"},
    {"name": "ehail_fee", "type": "FLOAT64"},
    {"name": "improvement_surcharge", "type": "FLOAT64"},
    {"name": "total_amount", "type": "FLOAT64"},
    {"name": "payment_type", "type": "INTEGER"},
    {"name": "trip_type", "type": "STRING"},
    {"name": "congestion_surcharge", "type": "FLOAT64"},
]


def get_columns(taxi: str) -> list[str]:
    """Return the column list for the given taxi type."""
    return YELLOW_COLUMNS if taxi == "yellow" else GREEN_COLUMNS


def get_dedup_columns(taxi: str) -> list[str]:
    """Return the deduplication columns for the given taxi type."""
    return YELLOW_DEDUP_COLS if taxi == "yellow" else GREEN_DEDUP_COLS


def get_bq_schema(taxi: str) -> list[dict]:
    """Return the BigQuery schema for the given taxi type."""
    return YELLOW_BQ_SCHEMA if taxi == "yellow" else GREEN_BQ_SCHEMA


def col_definitions(columns: list[str]) -> str:
    """Generate SQL column definitions (all TEXT)."""
    return ",\n    ".join(f'"{c}" TEXT' for c in columns)


def setup_postgres_conn():
    """Set the Airflow connection env var for the zoomcamp Postgres database."""
    os.environ["AIRFLOW_CONN_POSTGRES_ZOOMCAMP"] = (
        "postgresql://airflow:airflow@postgres:5432/zoomcamp"
    )
