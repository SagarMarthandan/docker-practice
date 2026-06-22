#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='taxi_zone_lookup', help='Target table name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading CSV')

def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, chunksize):
    """Ingest NYC taxi zone lookup data into PostgreSQL database."""
    
    # 1. Setup Source and Destination
    url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
    conn_string = f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'

    try:
        engine = create_engine(conn_string)
        engine.connect().close()
    except Exception as exc:
        raise click.ClickException(f"Cannot connect to PostgreSQL at {pg_host}:{pg_port}/{pg_db}: {exc}") from exc

    # 2. Initialize Data Stream
    try:
        df_iter = pd.read_csv(
            url,
            iterator=True,
            chunksize=chunksize,
        )
    except Exception as exc:
        raise click.ClickException(f"Failed to read CSV from {url}: {exc}") from exc

    first = True
    ingested_chunks = 0

    # 3. Batch Ingestion
    for df_chunk in tqdm(df_iter, desc="Ingesting zone data"):
        if first:
            # Create table structure (replace if exists)
            df_chunk.head(n=0).to_sql(
                name=target_table,
                con=engine,
                if_exists='replace'
            )
            first = False

        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists='append'
        )
        ingested_chunks += 1

    if ingested_chunks == 0:
        raise click.ClickException(f"No data found at {url}")

if __name__ == '__main__':
    run()