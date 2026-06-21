from airflow.decorators import dag, task
from airflow.models.param import Param
from datetime import datetime

@dag(
    dag_id="03_getting_started_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    tags=["zoomcamp"],
    catchup=False,
    params={
        # Kestra: inputs - id: columns_to_keep, type: ARRAY
        "columns_to_keep": Param(
            default=["brand", "price"],
            type="array",
            description="Columns to retain from the products JSON",
        )
    },
)
def data_pipeline():

    @task
    def extract() -> dict:
        """Equivalent of: type: io.kestra.plugin.core.http.Download"""
        import requests
        response = requests.get("https://dummyjson.com/products", timeout=10)
        response.raise_for_status()
        return response.json()

    @task
    def transform(raw_data: dict, **context) -> list:
        """Equivalent of: type: io.kestra.plugin.scripts.python.Script"""
        columns_to_keep = context["params"]["columns_to_keep"]
        products = raw_data.get("products", [])
        filtered = [
            {col: product.get(col, "N/A") for col in columns_to_keep}
            for product in products
        ]
        print(f"Filtered {len(filtered)} products, keeping columns: {columns_to_keep}")
        return filtered

    @task
    def store_in_postgres(filtered_data: list):
        """Stores the transformed data in the PostgreSQL database."""
        import os
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        if not filtered_data:
            print("No data to store.")
            return

        # Dynamically set Postgres connection pointing to the zoomcamp database
        os.environ["AIRFLOW_CONN_POSTGRES_ZOOMCAMP"] = "postgresql://airflow:airflow@postgres:5432/zoomcamp"
        
        pg_hook = PostgresHook(postgres_conn_id="postgres_zoomcamp")

        # Get dynamic columns based on data keys
        columns = list(filtered_data[0].keys())
        columns_sql = ", ".join([f'"{col}" VARCHAR(255)' for col in columns])

        # Drop and recreate table to ensure schema updates if columns change
        pg_hook.run("DROP TABLE IF EXISTS products;")
        pg_hook.run(f"CREATE TABLE products ({columns_sql});")

        # Format rows and insert
        rows_to_insert = [
            tuple(product.get(col) for col in columns)
            for product in filtered_data
        ]
        pg_hook.insert_rows(
            table="products",
            rows=rows_to_insert,
            target_fields=columns
        )
        print(f"Successfully inserted {len(rows_to_insert)} rows into Postgres 'products' table!")

        # Query back and print the first 5 rows to verify
        result = pg_hook.get_records("SELECT * FROM products LIMIT 5;")
        print("Postgres query result (first 5 rows):")
        for row in result:
            print(row)

    # Task dependencies — same as Kestra's sequential task list
    raw = extract()
    filtered = transform(raw)
    store_in_postgres(filtered)

data_pipeline()