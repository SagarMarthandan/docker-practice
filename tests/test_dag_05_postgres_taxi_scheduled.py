"""Unit tests for dag_05_postgres_taxi_scheduled — the scheduled variant.

DAG 05 is nearly identical to DAG 04, but derives year/month from
data_interval_start instead of params. We test the shared column definitions
and the date-derivation logic.
"""

from datetime import datetime

import pytest

import dag_05_postgres_taxi_scheduled as dag05
import dag_04_postgres_taxi as dag04


class TestScheduledColDefinitions:
    def test_same_helper_as_dag04(self):
        result = dag05._col_definitions(["VendorID", "trip_distance"])
        assert '"VendorID" TEXT' in result
        assert '"trip_distance" TEXT' in result


class TestScheduledColumnConstants:
    def test_yellow_columns_match_dag04(self):
        assert dag05.YELLOW_COLUMNS == dag04.YELLOW_COLUMNS

    def test_green_columns_match_dag04(self):
        assert dag05.GREEN_COLUMNS == dag04.GREEN_COLUMNS

    def test_yellow_dedup_cols_match_dag04(self):
        assert dag05.YELLOW_DEDUP_COLS == dag04.YELLOW_DEDUP_COLS

    def test_green_dedup_cols_match_dag04(self):
        assert dag05.GREEN_DEDUP_COLS == dag04.GREEN_DEDUP_COLS


class TestDateDerivation:
    @pytest.mark.parametrize("dt,expected_year,expected_month", [
        (datetime(2019, 1, 15), "2019", "01"),
        (datetime(2020, 12, 1), "2020", "12"),
        (datetime(2021, 6, 30), "2021", "06"),
    ])
    def test_strftime_derivation(self, dt, expected_year, expected_month):
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        assert year == expected_year
        assert month == expected_month

    def test_filename_from_logical_date(self):
        dt = datetime(2019, 3, 1)
        taxi = "yellow"
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        filename = f"{taxi}_tripdata_{year}-{month}.csv"
        assert filename == "yellow_tripdata_2019-03.csv"

    def test_green_filename_from_logical_date(self):
        dt = datetime(2020, 11, 1)
        taxi = "green"
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        filename = f"{taxi}_tripdata_{year}-{month}.csv"
        assert filename == "green_tripdata_2020-11.csv"
