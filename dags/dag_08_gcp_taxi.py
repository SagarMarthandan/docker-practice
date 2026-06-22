from airflow.decorators import dag, task
from airflow.models.param import Param
from datetime import datetime

from shared.extract import download_and_decompress
from shared.file_utils import remove_file
from shared.gcp_tasks import ensure_bq_table, load_gcs_uri_to_bq, upload_file_to_gcs


@dag(
    dag_id="08_gcp_taxi",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    max_active_runs=1,
    tags=["zoomcamp", "gcp"],
    catchup=False,
    params={
        "taxi":  Param("yellow", enum=["yellow", "green"]),
        "year":  Param("2019", type="string"),
        "month": Param("01",   type="string"),
    },
)
def gcp_taxi():

    @task
    def extract(**context) -> str:
        """Downloads and uncompresses the taxi trip data."""
        taxi = context["params"]["taxi"]
        year = context["params"]["year"]
        month = context["params"]["month"]
        return download_and_decompress(taxi, year, month)

    @task
    def upload_to_gcs(file_path: str, **context) -> str:
        """Uploads the local CSV file to GCS."""
        taxi = context["params"]["taxi"]
        year = context["params"]["year"]
        month = context["params"]["month"]
        return upload_file_to_gcs(file_path, taxi, year, month)

    @task
    def create_bq_table(**context):
        """Creates the destination BQ table if it doesn't exist."""
        taxi = context["params"]["taxi"]
        year = context["params"]["year"]
        month = context["params"]["month"]
        ensure_bq_table(taxi, year, month)

    @task
    def load_gcs_to_bq(gcs_uri: str, **context):
        """Loads data from GCS into BigQuery table."""
        taxi = context["params"]["taxi"]
        year = context["params"]["year"]
        month = context["params"]["month"]
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

gcp_taxi()
