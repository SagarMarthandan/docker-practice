"""Shared database utilities for pipeline ingestion scripts."""

import pandas as pd
from sqlalchemy import Engine, create_engine
from tqdm.auto import tqdm


def create_pg_engine(
    pg_user: str, pg_pass: str, pg_host: str, pg_port: int, pg_db: str
) -> Engine:
    """Create a SQLAlchemy engine for PostgreSQL."""
    conn_string = f"postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    return create_engine(conn_string)


def ingest_csv_chunked(
    url: str,
    engine: Engine,
    target_table: str,
    chunksize: int = 100000,
    dtype: dict | None = None,
    parse_dates: list[str] | None = None,
    description: str = "Ingesting data",
) -> None:
    """Download a CSV (or .csv.gz) in chunks and ingest into PostgreSQL."""
    read_kwargs: dict = {
        "iterator": True,
        "chunksize": chunksize,
    }
    if dtype:
        read_kwargs["dtype"] = dtype
    if parse_dates:
        read_kwargs["parse_dates"] = parse_dates

    df_iter = pd.read_csv(url, **read_kwargs)

    first = True
    for df_chunk in tqdm(df_iter, desc=description):
        if first:
            df_chunk.head(n=0).to_sql(
                name=target_table,
                con=engine,
                if_exists="replace",
            )
            first = False

        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
        )
