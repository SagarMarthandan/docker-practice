from airflow.decorators import dag, task
from airflow.models.param import Param
from datetime import datetime

from shared.extract import download_and_decompress
from shared.file_utils import remove_file
from shared.gcp_tasks import ensure_bq_table, load_gcs_uri_to_bq, upload_file_to_gcs


@dag(
    dag_id="09_gcp_taxi_scheduled",
    start_date=datetime(2019, 1, 1),
    schedule="0 9 1 * *",
    max_active_runs=1,
    catchup=True,
    tags=["zoomcamp", "gcp", "scheduled"],
    params={
        "taxi": Param("yellow", enum=["yellow", "green"]),
    },
)
def gcp_taxi_scheduled():

    @task
    def extract(**context) -> str:
        """Downloads and uncompresses the taxi trip data."""
        taxi = context["params"]["taxi"]
        logical_date = context["data_interval_start"]
        year = logical_date.strftime("%Y")
        month = logical_date.strftime("%m")
        return download_and_decompress(taxi, year, month)

    @task
    def upload_to_gcs(file_path: str, **context) -> str:
        """Uploads the local CSV file to GCS."""
        taxi = context["params"]["taxi"]
        logical_date = context["data_interval_start"]
        year = logical_date.strftime("%Y")
        month = logical_date.strftime("%m")
        return upload_file_to_gcs(file_path, taxi, year, month)

    @task
    def create_bq_table(**context):
        """Creates the destination BQ table if it doesn't exist."""
        taxi = context["params"]["taxi"]
        logical_date = context["data_interval_start"]
        year = logical_date.strftime("%Y")
        month = logical_date.strftime("%m")
        ensure_bq_table(taxi, year, month)

    @task
    def load_gcs_to_bq(gcs_uri: str, **context):
        """Loads data from GCS into BigQuery table."""
        taxi = context["params"]["taxi"]
        logical_date = context["data_interval_start"]
        year = logical_date.strftime("%Y")
        month = logical_date.strftime("%m")
        load_gcs_uri_to_bq(gcs_uri, taxi, year, month)

    @task
    def purge_file(file_path: str):
        """Remove temp file."""
        remove_file(file_path)

    # ── Wire tasks ──────────────────────────────────────────────────────────
    path = extract()
    gcs_uri = upload_to_gcs(path)
    bq_table = create_bq_table()
    load = load_gcs_to_bq(gcs_uri)
    purge = purge_file(path)

    bq_table >> load >> purge

gcp_taxi_scheduled()
