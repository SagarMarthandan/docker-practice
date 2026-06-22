#!/usr/bin/env python
# coding: utf-8

import click

from db_utils import create_pg_engine, ingest_csv_chunked


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
    url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
    engine = create_pg_engine(pg_user, pg_pass, pg_host, pg_port, pg_db)

    ingest_csv_chunked(
        url=url,
        engine=engine,
        target_table=target_table,
        chunksize=chunksize,
        description="Ingesting zone data",
    )


if __name__ == '__main__':
    run()
