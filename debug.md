# Airflow & PostgreSQL Docker Debugging Log

A summary of the errors encountered during the local setup of Apache Airflow and PostgreSQL 18, their root causes, and the changes applied to fix them.

---

## 1. PostgreSQL 18 Volume Mount Error

### 🔴 The Error
The `postgres` service container exited immediately with status code `1`. Checking `docker compose logs postgres` revealed:
```text
postgres-1  | Error: in 18+, these Docker images are configured to store database data in a
postgres-1  |        format which is compatible with "pg_ctlcluster" (specifically, using
postgres-1  |        major-version-specific directory names).
postgres-1  | 
postgres-1  |        Counter to that, there appears to be PostgreSQL data in:
postgres-1  |          /var/lib/postgresql/data (unused mount/volume)
postgres-1  | 
postgres-1  |        The suggested container configuration for 18+ is to place a single mount
postgres-1  |        at /var/lib/postgresql which will then place PostgreSQL data in a
postgres-1  |        subdirectory...
```

### 🔍 Root Cause
Starting with PostgreSQL 18, the official Docker images expect the volume to be mounted at the parent directory `/var/lib/postgresql` rather than `/var/lib/postgresql/data`. This allows PostgreSQL to isolate major-version-specific directories and perform tools like `pg_upgrade` smoothly without mount-point boundaries.

### 🛠️ The Fix
Modified `docker-compose.yml` to change the volume mapping for the `postgres` service:
```diff
    volumes:
-     - postgres_data:/var/lib/postgresql/data
+     - postgres_data:/var/lib/postgresql
      - ./init-db.sql:/docker-entrypoint-initdb.d/init-db.sql
```

---

## 2. Airflow UID 1000 / getpwuid Error

### 🔴 The Error
The `airflow-init` container exited with code `1`. The logs showed:
```text
airflow.exceptions.AirflowConfigException: The user that Airflow is running as has no username; you must run Airflow as a full user, with a username and home directory, in order for it to function properly.
...
KeyError: 'getpwuid(): uid not found: 1000'
```

### 🔍 Root Cause
In `.env`, `AIRFLOW_UID` was set to `1000` so that files generated inside docker match the host user permissions. 
However, in `docker-compose.yml`, the `airflow-init` service had `entrypoint: /bin/bash`. Overriding the entrypoint directly bypassed the official Airflow image entrypoint (`/entrypoint`), which dynamically generates a passwd entry for arbitrary UIDs using `nss_wrapper`. Without this entrypoint running first, Python's `getpass.getuser()` crashed because UID 1000 did not exist in `/etc/passwd`.

### 🛠️ The Fix
Removed the `entrypoint: /bin/bash` override from `airflow-init` so the default image entrypoint could run, and modified `command` to prepend `bash`:
```diff
  airflow-init:
    <<: *airflow-common
-   entrypoint: /bin/bash
    command:
+     - bash
      - -c
      - |
        airflow db migrate &&
        airflow users create \
...
```

---

## 3. Database Migration Race Condition

### 🔴 The Error
The Airflow Web UI crashed on startup, and running connection CLI commands returned:
```text
psycopg2.errors.UndefinedTable: relation "connection" does not exist
```

### 🔍 Root Cause
Because both `airflow-webserver` and `airflow-scheduler` inherited `depends_on: postgres` from `x-airflow-common`, they started up in parallel with `airflow-init`. 
When they started up, they queried the database and initialized Flask AppBuilder security tables before the main schema migration occurred. When `airflow-init` eventually ran `airflow db migrate`, Alembic detected pre-existing tables, assumed the database was already populated, and stamped the database revision as "done" (`Running stamp_revision`) without actually running the schema migrations. This left the database in a corrupted/half-migrated state missing core tables like `connection`.

### 🛠️ The Fix
1. Updated `docker-compose.yml` to specify that `airflow-webserver` and `airflow-scheduler` must wait until `airflow-init` exits successfully:
```diff
  airflow-webserver:
    <<: *airflow-common
    command: webserver
...
+   depends_on:
+     postgres:
+       condition: service_healthy
+     airflow-init:
+       condition: service_completed_successfully

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
+   depends_on:
+     postgres:
+       condition: service_healthy
+     airflow-init:
+       condition: service_completed_successfully
```
2. Ran `docker compose down -v` to delete the corrupted named database volume and restarted the services using `docker compose up -d` to ensure a clean, sequential migration.

---

## 4. Connection Testing Disabled

### 🔴 The Error
Attempting to test the Postgres connection in the Airflow UI or via CLI returned:
```text
Testing connections is disabled in Airflow configuration. Contact your deployment admin to enable it.
```

### 🔍 Root Cause
Airflow disables testing connections by default starting in newer versions to prevent security risks (e.g. Server-Side Request Forgery).

### 🛠️ The Fix
Added the `AIRFLOW__CORE__TEST_CONNECTION` environment variable under `x-airflow-common` in `docker-compose.yml`:
```diff
x-airflow-common: &airflow-common
  image: apache/airflow:2.9.3
  environment:
    ...
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG: 'true'
+   AIRFLOW__CORE__TEST_CONNECTION: 'Enabled'
```

---

## 5. Postgres Taxi Pipeline (DAG 04) Execution Errors

### 🔴 The Errors
Several consecutive task failures occurred when executing `dag_04_postgres_taxi.py`:
1. `AirflowNotFoundException: The conn_id 'postgres_zoomcamp' isn't defined`
2. `subprocess.CalledProcessError: Command 'wget ...' returned non-zero exit status 1`
3. `psycopg2.errors.BadCopyFileFormat: extra data after last expected column`
4. `psycopg2.errors.UndefinedColumn: column "ehail_fee" of relation "green_tripdata_staging" does not exist`

### 🔍 Root Cause
1. **Missing connection ID**: Airflow's metadata DB did not contain a configured connection named `postgres_zoomcamp` by default.
2. **Missing shell dependencies**: The official `apache/airflow` Docker image does not contain the `wget` utility.
3. **Column schema mismatch**: The `GREEN_COLUMNS` Python list was missing the `"ehail_fee"` column, which is present in the green taxi dataset (causing a count discrepancy of 20 vs 19 columns).
4. **Stale tables**: Because tables were created using `CREATE TABLE IF NOT EXISTS`, database tables were not updated to reflect schema changes (like adding `"ehail_fee"`) between runs.
5. **Race conditions**: Tasks were called in Python but the dependency operator (`>>`) was missing for `merge_to_final` and `purge_file`, which could lead to deleting temporary files before the copy/load task was finished.

### 🛠️ The Fix

#### 1. Injected Connection String
Dynamically configured `AIRFLOW_CONN_POSTGRES_ZOOMCAMP` at the top of [dag_04_postgres_taxi.py](file:///home/sagar/repos/docker-practice/dags/dag_04_postgres_taxi.py):
```python
import os

# Dynamically set Postgres connection pointing to the zoomcamp database
os.environ["AIRFLOW_CONN_POSTGRES_ZOOMCAMP"] = "postgresql://airflow:airflow@postgres:5432/zoomcamp"
```

#### 2. Replaced `wget` with Pure Python Download & Decompress
Rewrote the `extract()` task:
```python
    @task
    def extract(**context) -> str:
        """Downloads and uncompresses the taxi trip data using pure Python."""
        import urllib.request
        import gzip
        import shutil

        # ...
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req) as response, open(gz_out_path, 'wb') as out_file:
                shutil.copyfileobj(response, out_file)
            
            with gzip.open(gz_out_path, 'rb') as f_in:
                with open(out_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        finally:
            if os.path.exists(gz_out_path):
                os.remove(gz_out_path)
        return out_path
```

#### 3. Added `"ehail_fee"` to `GREEN_COLUMNS`
Updated the column schema list definition:
```python
GREEN_COLUMNS = [
    "VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime",
    "store_and_fwd_flag", "RatecodeID", "PULocationID", "DOLocationID",
    "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax",
    "tip_amount", "tolls_amount", "ehail_fee", "improvement_surcharge",
    "total_amount", "payment_type", "trip_type", "congestion_surcharge",
]
```

#### 4. Dropped Tables Before Creation
Modified the `create_tables()` task to drop the existing stale tables before recreate, forcing Postgres schema updates:
```python
        # Drop and recreate tables to ensure schema updates if columns change
        hook.run(f"DROP TABLE IF EXISTS public.{taxi}_tripdata_staging;")
        hook.run(f"""
            CREATE TABLE public.{taxi}_tripdata_staging (
                {extra_cols},
                {col_defs}
            );
        """)
```

#### 5. Fixed Task Chain Wiring
Changed the pipeline wiring at the end of the DAG definition:
```python
    path = extract()
    tables = create_tables()
    load = load_to_staging(path)
    merge = merge_to_final()
    purge = purge_file(path)

    # Explicit ordering to ensure sequential/correct execution
    tables >> load >> merge >> purge
```

---

## 6. GCP Credentials & Variable Persistence Error

### 🔴 The Error
When running `docker compose down -v` to reset or tear down containers, all connections (e.g. `google_cloud_default`) and variables (e.g. `GCP_PROJECT_ID`, `GCP_BUCKET_NAME`) configured manually in the Airflow UI or via CLI were lost, because the PostgreSQL metadata database volume (`postgres_data`) was completely deleted.

### 🔍 Root Cause
Airflow stores connections and variables in its metadata database. A teardown with the `-v` flag deletes all volumes associated with the services. As a result, the metadata database starts clean and empty on the next `docker compose up -d`, requiring manual reconfiguration of GCP settings.

### 🛠️ The Fix
We configured persistent connections and variables directly in `docker-compose.yml` under `x-airflow-common -> environment` and `volumes`. Airflow automatically registers these environment variables as connections and variables at runtime, ensuring they persist across database wipes:

1. **Mounted the GCP key file**:
   ```yaml
       volumes:
         - /home/sagar/dtc-de-course/keys/google_credentials.json:/opt/airflow/keys/google_credentials.json:ro
   ```
2. **Added `GOOGLE_APPLICATION_CREDENTIALS` and the connection string**:
   ```yaml
       environment:
         GOOGLE_APPLICATION_CREDENTIALS: /opt/airflow/keys/google_credentials.json
         AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT: '{"conn_type": "google_cloud_platform", "extra": {"project": "dtc-de-course-497317", "key_path": "/opt/airflow/keys/google_credentials.json"}}'
   ```
3. **Injected the key configuration variables**:
   ```yaml
         AIRFLOW_VAR_GCP_PROJECT_ID: "dtc-de-course-497317"
         AIRFLOW_VAR_GCP_BUCKET_NAME: "dtc-de-course-497317-terra-bucket"
         AIRFLOW_VAR_GCP_DATASET: "trips_data_all"
         AIRFLOW_VAR_GCP_LOCATION: "EU"
   ```

---

## 7. GCP Infrastructure & Data Pipelines (DAG 07, DAG 08 & DAG 09) Airflow Compliance

### 🔴 The Errors / Incompatibilities
When translating Kestra GCP setups and data workflows into Airflow ([dag_07_gcp_create_infra.py](file:///home/sagar/repos/docker-practice/dags/dag_07_gcp_create_infra.py), [dag_08_gcp_taxi.py](file:///home/sagar/repos/docker-practice/dags/dag_08_gcp_taxi.py), and [dag_09_gcp_taxi_scheduled.py](file:///home/sagar/repos/docker-practice/dags/dag_09_gcp_taxi_scheduled.py)), the initial versions suffered from several incompatibilities:
1. **Raw Google Client Library dependency**: The tasks directly imported `google.cloud.storage` and `google.cloud.bigquery`, which bypassed the native Airflow Connection configurations.
2. **Missing `wget` dependencies**: The download steps in DAG 08 & DAG 09 executed `wget` via shell commands inside `subprocess.run()`, which failed because `wget` is not installed on default Airflow Docker images.
3. **TaskFlow syntax duplicate task creation**: In DAG 08 & DAG 09, calling `create_bq_table()` during dependency wiring (`create_bq_table() >> load_gcs_to_bq(gcs_uri)`) triggered a duplicate task instance (`create_bq_table__1`) instead of referencing the existing task run.
4. **Green Taxi schema mismatch**: The `green_schema` was missing the `ehail_fee` column, which caused dataset loading jobs to crash.

### 🔍 Root Causes
1. Raw Google client calls don't automatically parse Airflow Connections (like `google_cloud_default`), forcing developers to manage JSON files manually inside Python scripts.
2. Official `apache/airflow` Docker images are minimized and do not ship with shell-based HTTP download command utilities.
3. In Airflow's TaskFlow API (`@task` decorator), calling a function returns a task invocation. Invoking it a second time initializes a brand-new task node in the DAG graph.
4. The green taxi dataset contains 20 columns, but the default schema config only defined 19 columns.

### 🛠️ The Fix
1. **Converted to Native Airflow Hooks**:
   - Swapped out raw client connections for `GCSHook` and `BigQueryHook`. The hooks automatically resolve credentials via `google_cloud_default`.
   - Checked bucket existence using `client.bucket(bucket_name).exists()` (retrieved via `hook.get_conn()`) instead of calling `hook.exists()`. In Airflow, `GCSHook.exists()` checks for the existence of an *object (file)* inside a bucket, which throws a `TypeError: missing object_name` if only passed a bucket name.
   - Used hook helpers such as `BigQueryHook.create_empty_table(..., exists_ok=True)` to simplify table creation checks.
2. **Rewrote Extractor in Pure Python**:
   - Re-implemented the `extract` tasks to use python's standard libraries (`urllib.request`, `gzip`, and `shutil`) to fetch and decompress target datasets.
3. **Corrected TaskFlow wiring**:
   - Assigned the returned task instance to a variable and wired it directly:
     ```python
     bq_table = create_bq_table()
     load = load_gcs_to_bq(gcs_uri)
     bq_table >> load
     ```
4. **Synchronized Schema**:
   - Added `bigquery.SchemaField("ehail_fee", "FLOAT64")` to `green_schema` in DAG 08 and DAG 09.

---

## 8. Database Access via CLI (pgcli)

### 🛠️ Connecting to PostgreSQL
Since the database initialization script ([init-db.sql](file:///home/sagar/repos/docker-practice/init-db.sql)) creates a separate database called `zoomcamp` for storing taxi datasets next to the standard `airflow` database, use the following `pgcli` commands from your host terminal to access them:

- **Connect to the Taxi Data database (`zoomcamp`)**:
  ```bash
  uv run --with pgcli --with psycopg-binary pgcli -h localhost -p 5432 -u airflow -d zoomcamp
  ```
  *(Password: `airflow`)*

- **Connect to the Airflow Metadata database (`airflow`)**:
  ```bash
  uv run --with pgcli --with psycopg-binary pgcli -h localhost -p 5432 -u airflow -d airflow
  ```
  *(Password: `airflow`)*

---

## 9. GCP Ingestion Pipeline Runtime Errors (DAG 07, DAG 08 & DAG 09)

### 🔴 9.1 GCSHook.exists() TypeError

#### The Error
Executing `create_gcs_bucket` in DAG 07 crashed with:
```text
TypeError: GCSHook.exists() missing 1 required positional argument: 'object_name'
```

#### 🔍 Root Cause
Airflow's native `GCSHook.exists(bucket_name, object_name)` checks whether a specific file (blob) exists inside GCS. It is not designed to check if the GCS bucket itself exists, so passing only `bucket_name` triggered a validation signature crash.

#### 🛠️ The Fix
Wired the bucket check using the underlying client connection retrieved from the hook:
```python
        client = hook.get_conn()
        bucket = client.bucket(bucket_name)
        if bucket.exists():
            print(f"Bucket gs://{bucket_name} already exists — skipping")
```

---

### 🔴 9.2 BigQuery Schema JSON Serialization Error

#### The Error
Executing `create_bq_table` in DAG 08 & DAG 09 crashed with:
```text
TypeError: Object of type SchemaField is not JSON serializable
```

#### 🔍 Root Cause
Passing complex Python objects (like `google.cloud.bigquery.SchemaField` classes) to Hook operations inside `@task` functions can fail if Airflow attempts to serialize task metadata as JSON.

#### 🛠️ The Fix
Replaced the `SchemaField` class instances with plain Python dictionary mappings:
```python
        yellow_schema = [
            {"name": "VendorID", "type": "STRING"},
            {"name": "tpep_pickup_datetime", "type": "TIMESTAMP"},
            # ...
        ]
```

---

### 🔴 9.3 File Deletion Race Condition (FileNotFoundError)

#### The Error
The `upload_to_gcs` task in DAG 08 & DAG 09 failed with:
```text
FileNotFoundError: [Errno 2] No such file or directory: '/tmp/yellow_tripdata_2019-01.csv'
```

#### 🔍 Root Cause
Since `purge_file` and `upload_to_gcs` both took `path` (from `extract`) as their inputs, and there was no explicit execution dependency defined between them, Airflow ran them in parallel. `purge_file` completed first, deleting the file before `upload_to_gcs` had a chance to stream it to GCS.

#### 🛠️ The Fix
Chained the TaskFlow tasks sequentially using execution operators to ensure that file deletion only happens after the BigQuery load completes:
```python
    bq_table >> load >> purge
```

