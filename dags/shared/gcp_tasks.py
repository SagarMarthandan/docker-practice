"""Shared GCP task logic for the taxi pipeline DAGs."""

from airflow.models import Variable

from shared.taxi_schema import get_bq_schema

GCP_CONN_ID = "google_cloud_default"


def upload_file_to_gcs(file_path: str, taxi: str, year: str, month: str) -> str:
    """Upload a local CSV to GCS and return the gs:// URI."""
    from airflow.providers.google.cloud.hooks.gcs import GCSHook

    bucket_name = Variable.get("GCP_BUCKET_NAME")
    filename = f"{taxi}_tripdata_{year}-{month}.csv"

    hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    hook.upload(
        bucket_name=bucket_name,
        object_name=filename,
        filename=file_path,
    )

    gcs_uri = f"gs://{bucket_name}/{filename}"
    print(f"Uploaded to {gcs_uri}")
    return gcs_uri


def ensure_bq_table(taxi: str, year: str, month: str) -> None:
    """Create the BigQuery destination table if it doesn't exist."""
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    project_id = Variable.get("GCP_PROJECT_ID")
    dataset_id = Variable.get("GCP_DATASET")
    table_id = f"{taxi}_tripdata_{year}_{month}"

    schema = get_bq_schema(taxi)

    hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
    hook.create_empty_table(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        schema_fields=schema,
        exists_ok=True,
    )
    print(f"BigQuery table verified/created: {project_id}.{dataset_id}.{table_id}")


def load_gcs_uri_to_bq(gcs_uri: str, taxi: str, year: str, month: str) -> None:
    """Load data from a GCS URI into the corresponding BigQuery table."""
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
    from google.cloud import bigquery

    project_id = Variable.get("GCP_PROJECT_ID")
    dataset_id = Variable.get("GCP_DATASET")
    table_id = f"{taxi}_tripdata_{year}_{month}"
    full_table = f"{project_id}.{dataset_id}.{table_id}"

    hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
    client = hook.get_client(project_id=project_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
        autodetect=False,
    )

    load_job = client.load_table_from_uri(gcs_uri, full_table, job_config=job_config)
    load_job.result()
    print(f"Loaded {load_job.output_rows} rows into {full_table}")
