from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime

@dag(
    dag_id="07_gcp_create_infra",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    tags=["zoomcamp", "gcp", "setup"],
    catchup=False,
)
def gcp_create_infra():

    @task
    def create_gcs_bucket():
        """Creates a GCS bucket using the Airflow GCSHook."""
        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        bucket_name = Variable.get("GCP_BUCKET_NAME")
        location    = Variable.get("GCP_LOCATION", default_var="EU")
        project_id  = Variable.get("GCP_PROJECT_ID")

        hook = GCSHook(gcp_conn_id="google_cloud_default")
        
        # Check if bucket already exists to avoid conflict
        client = hook.get_conn()
        bucket = client.bucket(bucket_name)
        if bucket.exists():
            print(f"Bucket gs://{bucket_name} already exists — skipping")
        else:
            hook.create_bucket(bucket_name=bucket_name, location=location, project_id=project_id)
            print(f"Bucket created: gs://{bucket_name}")

    @task
    def create_bq_dataset():
        """Creates a BigQuery dataset using the Airflow BigQueryHook."""
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        project_id = Variable.get("GCP_PROJECT_ID")
        dataset_id = Variable.get("GCP_DATASET")
        location   = Variable.get("GCP_LOCATION", default_var="EU")

        hook = BigQueryHook(gcp_conn_id="google_cloud_default")
        
        # create_empty_dataset with exists_ok=True handles pre-existing datasets gracefully
        hook.create_empty_dataset(
            dataset_id=dataset_id,
            project_id=project_id,
            location=location,
            exists_ok=True
        )
        print(f"BigQuery dataset verified/created: {project_id}.{dataset_id}")

    # Both tasks run in parallel as they have no inter-dependencies
    create_gcs_bucket()
    create_bq_dataset()

gcp_create_infra()