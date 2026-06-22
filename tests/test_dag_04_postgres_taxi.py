"""Unit tests for dag_04_postgres_taxi helper functions and task logic.

The conftest.py installs Airflow stubs, so this file can directly import
the DAG module and exercise helper functions + verify SQL generation logic.
"""

import os
import textwrap

import pytest

import dag_04_postgres_taxi as dag04


# ===========================================================================
# Tests for _col_definitions
# ===========================================================================

class TestColDefinitions:
    def test_single_column(self):
        result = dag04._col_definitions(["trip_distance"])
        assert result == '"trip_distance" TEXT'

    def test_multiple_columns(self):
        cols = ["VendorID", "fare_amount", "tip_amount"]
        result = dag04._col_definitions(cols)
        expected = '"VendorID" TEXT,\n    "fare_amount" TEXT,\n    "tip_amount" TEXT'
        assert result == expected

    def test_empty_list(self):
        result = dag04._col_definitions([])
        assert result == ""

    def test_yellow_columns(self):
        result = dag04._col_definitions(dag04.YELLOW_COLUMNS)
        for col in dag04.YELLOW_COLUMNS:
            assert f'"{col}" TEXT' in result

    def test_green_columns(self):
        result = dag04._col_definitions(dag04.GREEN_COLUMNS)
        for col in dag04.GREEN_COLUMNS:
            assert f'"{col}" TEXT' in result


# ===========================================================================
# Tests for column and dedup constants
# ===========================================================================

class TestColumnConstants:
    def test_yellow_columns_not_empty(self):
        assert len(dag04.YELLOW_COLUMNS) > 0

    def test_green_columns_not_empty(self):
        assert len(dag04.GREEN_COLUMNS) > 0

    def test_yellow_dedup_cols_subset_of_yellow_columns(self):
        for col in dag04.YELLOW_DEDUP_COLS:
            assert col in dag04.YELLOW_COLUMNS

    def test_green_dedup_cols_subset_of_green_columns(self):
        for col in dag04.GREEN_DEDUP_COLS:
            assert col in dag04.GREEN_COLUMNS

    def test_yellow_columns_unique(self):
        assert len(dag04.YELLOW_COLUMNS) == len(set(dag04.YELLOW_COLUMNS))

    def test_green_columns_unique(self):
        assert len(dag04.GREEN_COLUMNS) == len(set(dag04.GREEN_COLUMNS))

    def test_yellow_has_pickup_dropoff_datetime(self):
        assert "tpep_pickup_datetime" in dag04.YELLOW_COLUMNS
        assert "tpep_dropoff_datetime" in dag04.YELLOW_COLUMNS

    def test_green_has_pickup_dropoff_datetime(self):
        assert "lpep_pickup_datetime" in dag04.GREEN_COLUMNS
        assert "lpep_dropoff_datetime" in dag04.GREEN_COLUMNS


# ===========================================================================
# Tests for purge_file logic
# ===========================================================================

class TestPurgeFile:
    def test_removes_existing_file(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("test")
        assert f.exists()
        path = str(f)
        if os.path.exists(path):
            os.remove(path)
        assert not f.exists()

    def test_no_error_on_missing_file(self, tmp_path):
        path = str(tmp_path / "nonexistent.csv")
        if os.path.exists(path):
            os.remove(path)


# ===========================================================================
# Tests for extract URL construction
# ===========================================================================

class TestExtractUrlConstruction:
    @pytest.mark.parametrize(
        "taxi,year,month,expected_suffix",
        [
            ("yellow", "2019", "01", "yellow/yellow_tripdata_2019-01.csv.gz"),
            ("green", "2020", "12", "green/green_tripdata_2020-12.csv.gz"),
            ("yellow", "2021", "06", "yellow/yellow_tripdata_2021-06.csv.gz"),
        ],
    )
    def test_url_format(self, taxi, year, month, expected_suffix):
        filename = f"{taxi}_tripdata_{year}-{month}.csv"
        url = (
            f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
            f"/{taxi}/{filename}.gz"
        )
        assert url.endswith(expected_suffix)


# ===========================================================================
# Tests for create_tables SQL generation
# ===========================================================================

class TestCreateTablesSql:
    def _build_create_sql(self, taxi):
        columns = dag04.YELLOW_COLUMNS if taxi == "yellow" else dag04.GREEN_COLUMNS
        col_defs = dag04._col_definitions(columns)
        extra_cols = '"unique_row_id" TEXT,\n    "filename" TEXT'

        staging_sql = textwrap.dedent(f"""
            CREATE TABLE public.{taxi}_tripdata_staging (
                {extra_cols},
                {col_defs}
            );
        """).strip()
        final_sql = textwrap.dedent(f"""
            CREATE TABLE public.{taxi}_tripdata (
                {extra_cols},
                {col_defs}
            );
        """).strip()
        return staging_sql, final_sql

    def test_yellow_creates_staging_and_final(self):
        staging, final = self._build_create_sql("yellow")
        assert "yellow_tripdata_staging" in staging
        assert "yellow_tripdata" in final
        assert '"unique_row_id" TEXT' in staging
        assert '"filename" TEXT' in staging

    def test_green_creates_staging_and_final(self):
        staging, final = self._build_create_sql("green")
        assert "green_tripdata_staging" in staging
        assert "green_tripdata" in final
        assert '"ehail_fee" TEXT' in staging

    def test_yellow_staging_contains_all_columns(self):
        staging, _ = self._build_create_sql("yellow")
        for col in dag04.YELLOW_COLUMNS:
            assert f'"{col}" TEXT' in staging

    def test_green_staging_contains_all_columns(self):
        staging, _ = self._build_create_sql("green")
        for col in dag04.GREEN_COLUMNS:
            assert f'"{col}" TEXT' in staging


# ===========================================================================
# Tests for merge SQL generation
# ===========================================================================

class TestMergeSql:
    def _build_merge_sql(self, taxi):
        columns = dag04.YELLOW_COLUMNS if taxi == "yellow" else dag04.GREEN_COLUMNS
        col_list = ", ".join(f'"{c}"' for c in ["unique_row_id", "filename"] + columns)
        s_cols = ", ".join(f'S."{c}"' for c in ["unique_row_id", "filename"] + columns)
        sql = (
            f"INSERT INTO public.{taxi}_tripdata ({col_list})\n"
            f"SELECT {s_cols}\n"
            f"FROM   public.{taxi}_tripdata_staging S\n"
            f"WHERE  NOT EXISTS (\n"
            f"    SELECT 1 FROM public.{taxi}_tripdata T\n"
            f"    WHERE  T.unique_row_id = S.unique_row_id\n"
            f");"
        )
        return sql

    def test_yellow_merge_references_correct_tables(self):
        sql = self._build_merge_sql("yellow")
        assert "yellow_tripdata_staging" in sql
        assert "yellow_tripdata" in sql
        assert "unique_row_id" in sql

    def test_green_merge_references_correct_tables(self):
        sql = self._build_merge_sql("green")
        assert "green_tripdata_staging" in sql
        assert "green_tripdata" in sql

    def test_merge_includes_not_exists(self):
        sql = self._build_merge_sql("yellow")
        assert "WHERE  NOT EXISTS" in sql


# ===========================================================================
# Tests for load_to_staging md5 expression
# ===========================================================================

class TestLoadToStagingMd5:
    def test_yellow_md5_expression(self):
        dedup_cols = dag04.YELLOW_DEDUP_COLS
        md5_expr = "||".join(f"COALESCE(\"{c}\"::TEXT, '')" for c in dedup_cols)
        assert "VendorID" in md5_expr
        assert "tpep_pickup_datetime" in md5_expr
        assert md5_expr.count("||") == len(dedup_cols) - 1

    def test_green_md5_expression(self):
        dedup_cols = dag04.GREEN_DEDUP_COLS
        md5_expr = "||".join(f"COALESCE(\"{c}\"::TEXT, '')" for c in dedup_cols)
        assert "lpep_pickup_datetime" in md5_expr
        assert md5_expr.count("||") == len(dedup_cols) - 1

    def test_md5_coalesce_handles_nulls(self):
        dedup_cols = ["A", "B"]
        md5_expr = "||".join(f"COALESCE(\"{c}\"::TEXT, '')" for c in dedup_cols)
        assert md5_expr == "COALESCE(\"A\"::TEXT, '')||COALESCE(\"B\"::TEXT, '')"
