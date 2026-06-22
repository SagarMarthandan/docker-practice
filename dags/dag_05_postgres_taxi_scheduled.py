from airflow.decorators import dag, task
from airflow.models.param import Param
from datetime import datetime

from shared.taxi_schema import setup_postgres_conn
from shared.extract import download_and_decompress
from shared.file_utils import remove_file
from shared.postgres_tasks import (
    create_taxi_tables,
    load_csv_to_staging,
    merge_staging_to_final,
)

setup_postgres_conn()


@dag(
    dag_id="05_postgres_taxi_scheduled",
    start_date=datetime(2019, 1, 1),
    schedule="0 9 1 * *",
    max_active_runs=1,
    catchup=True,
    tags=["zoomcamp", "postgres", "scheduled"],
    params={
        "taxi": Param("yellow", enum=["yellow", "green"]),
    },
)
def postgres_taxi_scheduled():

    @task
    def extract(**context) -> str:
        """Downloads and uncompresses the taxi trip data."""
        taxi = context["params"]["taxi"]
        logical_date = context["data_interval_start"]
        year = logical_date.strftime("%Y")
        month = logical_date.strftime("%m")
        return download_and_decompress(taxi, year, month)

    @task
    def create_tables(**context):
        """Drop and recreate staging + final tables."""
        create_taxi_tables(context["params"]["taxi"])

    @task
    def load_to_staging(file_path: str, **context):
        """Truncate staging, COPY CSV, compute MD5 hash, tag filename."""
        taxi = context["params"]["taxi"]
        logical_date = context["data_interval_start"]
        year = logical_date.strftime("%Y")
        month = logical_date.strftime("%m")
        load_csv_to_staging(file_path, taxi, year, month)

    @task
    def merge_to_final(**context):
        """Deduplicated merge from staging into final table."""
        merge_staging_to_final(context["params"]["taxi"])

    @task
    def purge_file(file_path: str):
        """Remove temp file."""
        remove_file(file_path)

    # ── Wire tasks ──────────────────────────────────────────────────────────
    path = extract()
    tables = create_tables()
    load = load_to_staging(path)
    merge = merge_to_final()
    purge = purge_file(path)

    tables >> load >> merge >> purge


postgres_taxi_scheduled()
