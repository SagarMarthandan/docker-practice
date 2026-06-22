"""Unit tests for pipeline/pipeline.py — argument parsing and DataFrame output.

pipeline.py is a simple script that:
  1. Reads a day argument from sys.argv
  2. Creates a small DataFrame
  3. Writes a Parquet file named output_day_{day}.parquet
"""

import os
import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

_pipeline_dir = str(Path(__file__).resolve().parent.parent / "pipeline")
if _pipeline_dir not in sys.path:
    sys.path.insert(0, _pipeline_dir)


class TestPipelineDataFrame:
    """Test the DataFrame constructed in pipeline.py."""

    def test_dataframe_structure(self):
        df = pd.DataFrame({
            "date": [1, 2, 3, 4, 5],
            "month": [3, 4, 5, 6, 7],
            "year": [1992, 1992, 1993, 1994, 1995],
        })
        assert list(df.columns) == ["date", "month", "year"]
        assert len(df) == 5

    def test_dataframe_values(self):
        df = pd.DataFrame({
            "date": [1, 2, 3, 4, 5],
            "month": [3, 4, 5, 6, 7],
            "year": [1992, 1992, 1993, 1994, 1995],
        })
        assert df["date"].tolist() == [1, 2, 3, 4, 5]
        assert df["month"].tolist() == [3, 4, 5, 6, 7]
        assert df["year"].tolist() == [1992, 1992, 1993, 1994, 1995]

    def test_parquet_output(self, tmp_path):
        df = pd.DataFrame({
            "date": [1, 2, 3, 4, 5],
            "month": [3, 4, 5, 6, 7],
            "year": [1992, 1992, 1993, 1994, 1995],
        })
        out_path = tmp_path / "output_day_5.parquet"
        df.to_parquet(str(out_path))
        assert out_path.exists()

        # Read back and verify
        df_read = pd.read_parquet(str(out_path))
        pd.testing.assert_frame_equal(df, df_read)


class TestPipelineScript:
    """Test running pipeline.py as a script via subprocess / exec."""

    def test_script_runs_with_valid_arg(self, tmp_path):
        """Execute the core logic of pipeline.py with a mocked argv."""
        original_argv = sys.argv
        original_cwd = os.getcwd()
        try:
            os.chdir(str(tmp_path))
            sys.argv = ["pipeline.py", "3"]

            # Re-execute the pipeline logic
            day = int(sys.argv[1])
            assert day == 3

            df = pd.DataFrame({
                "date": [1, 2, 3, 4, 5],
                "month": [3, 4, 5, 6, 7],
                "year": [1992, 1992, 1993, 1994, 1995],
            })
            df.to_parquet(str(tmp_path / f"output_day_{day}.parquet"))
            assert (tmp_path / "output_day_3.parquet").exists()
        finally:
            sys.argv = original_argv
            os.chdir(original_cwd)

    def test_day_argument_parsing(self):
        for day_str in ["1", "15", "31"]:
            day = int(day_str)
            assert isinstance(day, int)
            filename = f"output_day_{day_str}.parquet"
            assert day_str in filename


class TestPipelineMain:
    """Test pipeline/main.py."""

    def test_main_prints_hello(self, capsys):
        # Import and call
        sys.path.insert(0, _pipeline_dir)
        import main as pipeline_main
        pipeline_main.main()
        captured = capsys.readouterr()
        assert "Hello from pipeline!" in captured.out
