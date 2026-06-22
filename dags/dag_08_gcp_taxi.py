from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param
from datetime import datetime
import logging
import os

log = logging.getLogger(__name__)

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
        """Downloads and uncompresses the taxi trip data using pure Python."""
        import urllib.request
        import gzip
        import shutil

        taxi  = context["params"]["taxi"]
        year  = context["params"]["year"]
        month = context["params"]["month"]

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
    def upload_to_gcs(file_path: str, **context) -> str:
        """Uploads the local CSV file to GCS using GCSHook."""
        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        taxi  = context["params"]["taxi"]
        year  = context["params"]["year"]
        month = context["params"]["month"]

        bucket_name = Variable.get("GCP_BUCKET_NAME")
        filename    = f"{taxi}_tripdata_{year}-{month}.csv"
        gcs_path    = filename                          # object name in the bucket

        hook = GCSHook(gcp_conn_id="google_cloud_default")
        hook.upload(
            bucket_name=bucket_name,
            object_name=gcs_path,
            filename=file_path
        )

        gcs_uri = f"gs://{bucket_name}/{gcs_path}"
        print(f"Uploaded to {gcs_uri}")
        return gcs_uri

    @task
    def create_bq_table(**context):
        """Creates the destination BQ table if it doesn't exist using BigQueryHook."""
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        taxi  = context["params"]["taxi"]
        year  = context["params"]["year"]
        month = context["params"]["month"]

        project_id = Variable.get("GCP_PROJECT_ID")
        dataset_id = Variable.get("GCP_DATASET")
        table_id   = f"{taxi}_tripdata_{year}_{month}"

        # Schema represented as serializable dictionary structures
        yellow_schema = [
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
        green_schema = [
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

        schema = yellow_schema if taxi == "yellow" else green_schema
        
        hook = BigQueryHook(gcp_conn_id="google_cloud_default")
        hook.create_empty_table(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            schema_fields=schema,
            exists_ok=True
        )
        print(f"BigQuery table verified/created: {project_id}.{dataset_id}.{table_id}")

    @task
    def load_gcs_to_bq(gcs_uri: str, **context):
        """Loads data from GCS into BigQuery table."""
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        from google.cloud import bigquery

        taxi  = context["params"]["taxi"]
        year  = context["params"]["year"]
        month = context["params"]["month"]

        project_id = Variable.get("GCP_PROJECT_ID")
        dataset_id = Variable.get("GCP_DATASET")
        table_id   = f"{taxi}_tripdata_{year}_{month}"
        full_table = f"{project_id}.{dataset_id}.{table_id}"

        hook = BigQueryHook(gcp_conn_id="google_cloud_default")
        client = hook.get_client(project_id=project_id)
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,             # skip header
            write_disposition="WRITE_APPEND",
            autodetect=False,                # use the schema we already created
        )

        load_job = client.load_table_from_uri(gcs_uri, full_table, job_config=job_config)
        load_job.result()  # wait for completion
        print(f"Loaded {load_job.output_rows} rows into {full_table}")

    @task
    def purge_file(file_path: str):
        if os.path.exists(file_path):
            os.remove(file_path)
            log.info("Cleaned up: %s", file_path)
        else:
            log.warning("Expected temp file not found, may indicate an upstream issue: %s", file_path)

    # ── Wire tasks ──────────────────────────────────────────────────────────
    path     = extract()
    gcs_uri  = upload_to_gcs(path)
    bq_table = create_bq_table()
    load     = load_gcs_to_bq(gcs_uri)
    purge    = purge_file(path)

    # Chaining dependencies:
    # 1. create_bq_table must finish before loading
    # 2. purge_file must only run after loading is complete
    bq_table >> load >> purge

gcp_taxi()