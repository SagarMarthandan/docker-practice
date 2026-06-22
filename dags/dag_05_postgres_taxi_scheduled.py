from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os

# Dynamically set Postgres connection pointing to the zoomcamp database
os.environ["AIRFLOW_CONN_POSTGRES_ZOOMCAMP"] = "postgresql://airflow:airflow@postgres:5432/zoomcamp"

# ── Column definitions per taxi type ──────────────────────────────────────────
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
    "PULocationID", "DOLocationID", "fare_amount", "trip_distance"
]
GREEN_DEDUP_COLS = [
    "VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime",
    "PULocationID", "DOLocationID", "fare_amount", "trip_distance"
]


def _col_definitions(columns: list[str]) -> str:
    """Generate SQL column definitions (all TEXT for simplicity, matching Kestra's COPY approach)."""
    defs = ",\n    ".join(f'"{c}" TEXT' for c in columns)
    return defs


@dag(
    dag_id="05_postgres_taxi_scheduled",
    start_date=datetime(2019, 1, 1),   # set to earliest data you want to backfill from
    schedule="0 9 1 * *",             # Kestra: schedule: cron: "0 9 1 * *"
    max_active_runs=1,                # Kestra: concurrency: limit: 1
    catchup=True,                     # enables backfill — runs for all past intervals
    tags=["zoomcamp", "postgres", "scheduled"],
    params={
        # taxi type is still a manual input; year/month come from the schedule
        "taxi": Param("yellow", enum=["yellow", "green"]),
    },
)
def postgres_taxi_scheduled():

    @task
    def extract(**context) -> str:
        """Downloads and uncompresses the taxi trip data using pure Python."""
        import urllib.request
        import gzip
        import shutil

        taxi = context["params"]["taxi"]
        if taxi not in ("yellow", "green"):
            raise ValueError(f"Invalid taxi type: {taxi!r}")
        # Airflow: data_interval_start gives the logical execution date
        logical_date = context["data_interval_start"]
        year  = logical_date.strftime("%Y")
        month = logical_date.strftime("%m")

        filename = f"{taxi}_tripdata_{year}-{month}.csv"
        url = (
            f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
            f"/{taxi}/{filename}.gz"
        )
        gz_out_path = f"/tmp/{filename}.gz"
        out_path = f"/tmp/{filename}"

        print(f"Downloading {url} → {gz_out_path}")
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req) as response, open(gz_out_path, 'wb') as out_file:
                shutil.copyfileobj(response, out_file)
            
            print(f"Decompressing {gz_out_path} → {out_path}")
            with gzip.open(gz_out_path, 'rb') as f_in:
                with open(out_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        finally:
            if os.path.exists(gz_out_path):
                os.remove(gz_out_path)

        size = os.path.getsize(out_path)
        print(f"Downloaded {size:,} bytes uncompressed")
        return out_path

    @task
    def create_tables(**context):
        """
        Kestra: io.kestra.plugin.jdbc.postgresql.Queries
          - CREATE TABLE IF NOT EXISTS ... (staging + final)
        """
        taxi = context["params"]["taxi"]
        columns = YELLOW_COLUMNS if taxi == "yellow" else GREEN_COLUMNS
        col_defs = _col_definitions(columns)
        extra_cols = '"unique_row_id" TEXT,\n    "filename" TEXT'

        hook = PostgresHook(postgres_conn_id="postgres_zoomcamp")

        # Drop and recreate tables to ensure schema updates if columns change
        hook.run(f"DROP TABLE IF EXISTS public.{taxi}_tripdata_staging;")
        hook.run(f"""
            CREATE TABLE public.{taxi}_tripdata_staging (
                {extra_cols},
                {col_defs}
            );
        """)

        # Final deduplicated table
        hook.run(f"DROP TABLE IF EXISTS public.{taxi}_tripdata;")
        hook.run(f"""
            CREATE TABLE public.{taxi}_tripdata (
                {extra_cols},
                {col_defs}
            );
        """)
        print(f"Tables ready: {taxi}_tripdata_staging, {taxi}_tripdata")

    @task
    def load_to_staging(file_path: str, **context):
        """
        Kestra: TRUNCATE staging → COPY FROM csv → UPDATE md5 hash → UPDATE filename
        """
        taxi  = context["params"]["taxi"]
        logical_date = context["data_interval_start"]
        year  = logical_date.strftime("%Y")
        month = logical_date.strftime("%m")
        filename = f"{taxi}_tripdata_{year}-{month}.csv"
        columns  = YELLOW_COLUMNS if taxi == "yellow" else GREEN_COLUMNS

        hook = PostgresHook(postgres_conn_id="postgres_zoomcamp")
        conn = hook.get_conn()
        cur  = conn.cursor()

        # 1. Truncate staging
        cur.execute(f"TRUNCATE TABLE public.{taxi}_tripdata_staging;")

        # 2. COPY CSV into staging
        col_list = ", ".join(f'"{c}"' for c in columns)
        with open(file_path, "r") as f:
            copy_sql = (
                f"COPY public.{taxi}_tripdata_staging ({col_list}) "
                f"FROM STDIN WITH CSV HEADER"
            )
            cur.copy_expert(copy_sql, f)

        row_count = cur.rowcount
        print(f"Loaded {row_count:,} rows into staging")

        # 3. Add unique_row_id (MD5 of key fields)
        dedup_cols = YELLOW_DEDUP_COLS if taxi == "yellow" else GREEN_DEDUP_COLS
        md5_expr = "||".join(f"COALESCE(\"{c}\"::TEXT, '')" for c in dedup_cols)
        cur.execute(f"""
            UPDATE public.{taxi}_tripdata_staging
            SET unique_row_id = md5({md5_expr});
        """)

        # 4. Add filename tag
        cur.execute(f"""
            UPDATE public.{taxi}_tripdata_staging
            SET filename = '{filename}';
        """)

        conn.commit()
        cur.close()
        conn.close()

    @task
    def merge_to_final(**context):
        """
        Kestra: INSERT INTO final SELECT ... FROM staging
                WHERE NOT EXISTS (same unique_row_id in final)
        """
        taxi    = context["params"]["taxi"]
        columns = YELLOW_COLUMNS if taxi == "yellow" else GREEN_COLUMNS
        col_list = ", ".join(f'"{c}"' for c in ["unique_row_id", "filename"] + columns)
        s_cols   = ", ".join(f'S."{c}"' for c in ["unique_row_id", "filename"] + columns)

        hook = PostgresHook(postgres_conn_id="postgres_zoomcamp")
        hook.run(f"""
            INSERT INTO public.{taxi}_tripdata ({col_list})
            SELECT {s_cols}
            FROM   public.{taxi}_tripdata_staging S
            WHERE  NOT EXISTS (
                SELECT 1 FROM public.{taxi}_tripdata T
                WHERE  T.unique_row_id = S.unique_row_id
            );
        """)
        print(f"Merge complete → public.{taxi}_tripdata")

    @task
    def purge_file(file_path: str):
        """Remove temp file"""
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"Cleaned up: {file_path}")

    # ── Wire tasks ──────────────────────────────────────────────────────────
    path = extract()
    tables = create_tables()
    load = load_to_staging(path)
    merge = merge_to_final()
    purge = purge_file(path)

    # Explicit ordering to ensure sequential/correct execution
    tables >> load >> merge >> purge


postgres_taxi_scheduled()