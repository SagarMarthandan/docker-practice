"""Shared Postgres task logic for the taxi pipeline DAGs."""

from airflow.providers.postgres.hooks.postgres import PostgresHook

from shared.taxi_schema import col_definitions, get_columns, get_dedup_columns

POSTGRES_CONN_ID = "postgres_zoomcamp"


def create_taxi_tables(taxi: str) -> None:
    """Drop and recreate staging + final tables for the given taxi type."""
    columns = get_columns(taxi)
    col_defs = col_definitions(columns)
    extra_cols = '"unique_row_id" TEXT,\n    "filename" TEXT'

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    hook.run(f"DROP TABLE IF EXISTS public.{taxi}_tripdata_staging;")
    hook.run(f"""
        CREATE TABLE public.{taxi}_tripdata_staging (
            {extra_cols},
            {col_defs}
        );
    """)

    hook.run(f"DROP TABLE IF EXISTS public.{taxi}_tripdata;")
    hook.run(f"""
        CREATE TABLE public.{taxi}_tripdata (
            {extra_cols},
            {col_defs}
        );
    """)
    print(f"Tables ready: {taxi}_tripdata_staging, {taxi}_tripdata")


def load_csv_to_staging(file_path: str, taxi: str, year: str, month: str) -> None:
    """Truncate staging, COPY CSV, compute MD5 hash, tag filename."""
    filename = f"{taxi}_tripdata_{year}-{month}.csv"
    columns = get_columns(taxi)

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(f"TRUNCATE TABLE public.{taxi}_tripdata_staging;")

    col_list = ", ".join(f'"{c}"' for c in columns)
    with open(file_path, "r") as f:
        copy_sql = (
            f"COPY public.{taxi}_tripdata_staging ({col_list}) "
            f"FROM STDIN WITH CSV HEADER"
        )
        cur.copy_expert(copy_sql, f)

    row_count = cur.rowcount
    print(f"Loaded {row_count:,} rows into staging")

    dedup_cols = get_dedup_columns(taxi)
    md5_expr = "||".join(f"COALESCE(\"{c}\"::TEXT, '')" for c in dedup_cols)
    cur.execute(f"""
        UPDATE public.{taxi}_tripdata_staging
        SET unique_row_id = md5({md5_expr});
    """)

    cur.execute(f"""
        UPDATE public.{taxi}_tripdata_staging
        SET filename = '{filename}';
    """)

    conn.commit()
    cur.close()
    conn.close()


def merge_staging_to_final(taxi: str) -> None:
    """INSERT INTO final from staging WHERE NOT EXISTS (dedup by unique_row_id)."""
    columns = get_columns(taxi)
    col_list = ", ".join(f'"{c}"' for c in ["unique_row_id", "filename"] + columns)
    s_cols = ", ".join(f'S."{c}"' for c in ["unique_row_id", "filename"] + columns)

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
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
