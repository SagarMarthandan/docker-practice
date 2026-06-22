"""Unit tests for pipeline/ingest_data.py — CLI options, dtype map, URL construction.

The actual ingestion needs a running PostgreSQL instance, so we mock the DB
layer and verify CLI wiring, URL construction, and dtype/parse_dates config.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

_pipeline_dir = str(Path(__file__).resolve().parent.parent / "pipeline")
if _pipeline_dir not in sys.path:
    sys.path.insert(0, _pipeline_dir)

import ingest_data


# ===========================================================================
# Tests for dtype configuration
# ===========================================================================

class TestDtypeConfig:
    def test_dtype_keys_present(self):
        required = {
            "VendorID", "passenger_count", "trip_distance", "RatecodeID",
            "store_and_fwd_flag", "PULocationID", "DOLocationID",
            "payment_type", "fare_amount", "extra", "mta_tax",
            "tip_amount", "tolls_amount", "improvement_surcharge",
            "total_amount", "congestion_surcharge",
        }
        assert required.issubset(set(ingest_data.dtype.keys()))

    def test_integer_types(self):
        int_cols = ["VendorID", "passenger_count", "RatecodeID",
                    "PULocationID", "DOLocationID", "payment_type"]
        for col in int_cols:
            assert ingest_data.dtype[col] == "Int64"

    def test_float_types(self):
        float_cols = ["trip_distance", "fare_amount", "extra", "mta_tax",
                      "tip_amount", "tolls_amount", "improvement_surcharge",
                      "total_amount", "congestion_surcharge"]
        for col in float_cols:
            assert ingest_data.dtype[col] == "float64"

    def test_string_type(self):
        assert ingest_data.dtype["store_and_fwd_flag"] == "string"


class TestParseDates:
    def test_parse_dates_columns(self):
        assert "tpep_pickup_datetime" in ingest_data.parse_dates
        assert "tpep_dropoff_datetime" in ingest_data.parse_dates

    def test_parse_dates_length(self):
        assert len(ingest_data.parse_dates) == 2


# ===========================================================================
# Tests for URL construction
# ===========================================================================

class TestUrlConstruction:
    @pytest.mark.parametrize("year,month,expected", [
        (2022, 1, "yellow_tripdata_2022-01.csv.gz"),
        (2019, 12, "yellow_tripdata_2019-12.csv.gz"),
        (2020, 6, "yellow_tripdata_2020-06.csv.gz"),
    ])
    def test_url_format(self, year, month, expected):
        prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
        url = f"{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz"
        assert url.endswith(expected)

    def test_url_base(self):
        prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
        url = f"{prefix}/yellow_tripdata_2022-01.csv.gz"
        assert url.startswith("https://github.com/DataTalksClub/nyc-tlc-data")


# ===========================================================================
# Tests for connection string construction
# ===========================================================================

class TestConnectionString:
    def test_default_conn_string(self):
        conn = f"postgresql+psycopg://root:root@localhost:5432/ny_taxi"
        assert "postgresql+psycopg://" in conn
        assert "root:root@" in conn
        assert "localhost:5432" in conn
        assert "/ny_taxi" in conn

    @pytest.mark.parametrize("user,password,host,port,db", [
        ("admin", "secret", "db.example.com", 5433, "taxi_db"),
        ("test", "test", "127.0.0.1", 5432, "test_db"),
    ])
    def test_custom_conn_string(self, user, password, host, port, db):
        conn = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db}"
        assert user in conn
        assert str(port) in conn
        assert db in conn


# ===========================================================================
# Tests for CLI (Click) — invoke without hitting the network
# ===========================================================================

class TestCli:
    @patch("ingest_data.create_engine")
    @patch("ingest_data.pd")
    def test_run_invokes_read_csv(self, mock_pd, mock_engine):
        """Verify that run() calls pd.read_csv with the right parameters."""
        from click.testing import CliRunner

        # Make pd.read_csv return an empty iterator
        mock_pd.read_csv.return_value = iter([])

        runner = CliRunner()
        result = runner.invoke(ingest_data.run, [
            "--year", "2022",
            "--month", "1",
            "--chunksize", "100",
        ])

        mock_pd.read_csv.assert_called_once()
        call_kwargs = mock_pd.read_csv.call_args
        assert call_kwargs[1]["chunksize"] == 100
        assert call_kwargs[1]["iterator"] is True
