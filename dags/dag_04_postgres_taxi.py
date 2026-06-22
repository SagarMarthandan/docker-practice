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
    dag_id="04_postgres_taxi",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    max_active_runs=1,
    tags=["zoomcamp", "postgres"],
    catchup=False,
    params={
        "taxi":  Param("yellow", enum=["yellow", "green"], description="Taxi type"),
        "year":  Param("2019", type="string", description="Year (e.g. 2019)"),
        "month": Param("01",   type="string", description="Month zero-padded (e.g. 01)"),
    },
)
def postgres_taxi():

    @task
    def extract(**context) -> str:
        """Downloads and uncompresses the taxi trip data."""
        taxi = context["params"]["taxi"]
        year = context["params"]["year"]
        month = context["params"]["month"]
        return download_and_decompress(taxi, year, month)

    @task
    def create_tables(**context):
        """Drop and recreate staging + final tables."""
        create_taxi_tables(context["params"]["taxi"])

    @task
    def load_to_staging(file_path: str, **context):
        """Truncate staging, COPY CSV, compute MD5 hash, tag filename."""
        taxi = context["params"]["taxi"]
        year = context["params"]["year"]
        month = context["params"]["month"]
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

postgres_taxi()
