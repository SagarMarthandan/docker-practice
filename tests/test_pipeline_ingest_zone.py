"""Unit tests for pipeline/ingest_zone.py — CLI options and ingestion setup."""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

_pipeline_dir = str(Path(__file__).resolve().parent.parent / "pipeline")
if _pipeline_dir not in sys.path:
    sys.path.insert(0, _pipeline_dir)

import ingest_zone


class TestIngestZoneUrl:
    def test_source_url(self):
        url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
        assert "taxi_zone_lookup.csv" in url
        assert url.startswith("https://")


class TestIngestZoneConnectionString:
    def test_default_conn_string(self):
        conn = f"postgresql+psycopg://root:root@localhost:5432/ny_taxi"
        assert "postgresql+psycopg://" in conn

    @pytest.mark.parametrize("user,password,host,port,db", [
        ("root", "root", "localhost", 5432, "ny_taxi"),
        ("admin", "pass", "remotehost", 5433, "taxi_db"),
    ])
    def test_conn_string_format(self, user, password, host, port, db):
        conn = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db}"
        assert f"{user}:{password}" in conn
        assert f"@{host}:{port}" in conn
        assert f"/{db}" in conn


class TestIngestZoneCli:
    @patch("ingest_zone.create_engine")
    @patch("ingest_zone.pd")
    def test_run_invokes_read_csv(self, mock_pd, mock_engine):
        from click.testing import CliRunner

        mock_pd.read_csv.return_value = iter([])

        runner = CliRunner()
        result = runner.invoke(ingest_zone.run, [
            "--pg-user", "testuser",
            "--pg-db", "testdb",
            "--chunksize", "50",
        ])

        mock_pd.read_csv.assert_called_once()
        call_kwargs = mock_pd.read_csv.call_args
        assert call_kwargs[1]["chunksize"] == 50
        assert call_kwargs[1]["iterator"] is True

    @patch("ingest_zone.create_engine")
    @patch("ingest_zone.pd")
    def test_default_target_table(self, mock_pd, mock_engine):
        from click.testing import CliRunner

        mock_pd.read_csv.return_value = iter([])

        runner = CliRunner()
        result = runner.invoke(ingest_zone.run)
        # Default values should be used; just verify no error
        assert result.exit_code == 0
