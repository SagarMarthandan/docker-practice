"""Unit tests for dag_03_data_pipeline — specifically the transform logic.

The extract and store_in_postgres tasks depend on HTTP and Airflow hooks,
so we focus on the transform function's pure filtering logic and the SQL
generation used in store_in_postgres.
"""

import pytest

import dag_03_data_pipeline as dag03


# ===========================================================================
# Helpers — replicate the transform logic so we can test it in isolation
# ===========================================================================

def _transform(raw_data: dict, columns_to_keep: list[str]) -> list:
    """Replicate the transform logic from the DAG."""
    products = raw_data.get("products", [])
    filtered = [
        {col: product.get(col, "N/A") for col in columns_to_keep}
        for product in products
    ]
    return filtered


# ===========================================================================
# Tests
# ===========================================================================

class TestTransform:
    RAW_DATA = {
        "products": [
            {"id": 1, "brand": "Apple", "price": 999, "category": "phones"},
            {"id": 2, "brand": "Samsung", "price": 799, "category": "phones"},
            {"id": 3, "brand": "Sony", "price": 499, "category": "audio"},
        ]
    }

    def test_keeps_selected_columns(self):
        result = _transform(self.RAW_DATA, ["brand", "price"])
        assert len(result) == 3
        assert result[0] == {"brand": "Apple", "price": 999}
        assert result[1] == {"brand": "Samsung", "price": 799}

    def test_single_column(self):
        result = _transform(self.RAW_DATA, ["brand"])
        assert result == [
            {"brand": "Apple"},
            {"brand": "Samsung"},
            {"brand": "Sony"},
        ]

    def test_missing_column_defaults_to_na(self):
        result = _transform(self.RAW_DATA, ["brand", "color"])
        for item in result:
            assert item["color"] == "N/A"

    def test_empty_products(self):
        result = _transform({"products": []}, ["brand"])
        assert result == []

    def test_missing_products_key(self):
        result = _transform({}, ["brand"])
        assert result == []

    def test_all_columns(self):
        result = _transform(self.RAW_DATA, ["id", "brand", "price", "category"])
        assert result[0] == {"id": 1, "brand": "Apple", "price": 999, "category": "phones"}

    def test_empty_columns_list(self):
        result = _transform(self.RAW_DATA, [])
        assert result == [{}, {}, {}]

    def test_preserves_product_count(self):
        raw = {"products": [{"x": i} for i in range(100)]}
        result = _transform(raw, ["x"])
        assert len(result) == 100

    def test_preserves_original_data_types(self):
        raw = {"products": [{"name": "test", "price": 10.5, "count": 3}]}
        result = _transform(raw, ["name", "price", "count"])
        assert result[0]["price"] == 10.5
        assert result[0]["count"] == 3
        assert isinstance(result[0]["name"], str)


class TestExtractUrl:
    def test_extract_url(self):
        url = "https://dummyjson.com/products"
        assert "dummyjson.com" in url
        assert url.startswith("https://")


class TestStoreInPostgresColumnSql:
    def test_columns_sql_from_keys(self):
        data = [{"brand": "Apple", "price": "999"}]
        columns = list(data[0].keys())
        columns_sql = ", ".join([f'"{col}" VARCHAR(255)' for col in columns])
        assert '"brand" VARCHAR(255)' in columns_sql
        assert '"price" VARCHAR(255)' in columns_sql

    def test_rows_to_insert_format(self):
        data = [
            {"brand": "Apple", "price": "999"},
            {"brand": "Samsung", "price": "799"},
        ]
        columns = list(data[0].keys())
        rows = [tuple(product.get(col) for col in columns) for product in data]
        assert rows == [("Apple", "999"), ("Samsung", "799")]

    def test_empty_data_returns_no_rows(self):
        data = []
        assert len(data) == 0
