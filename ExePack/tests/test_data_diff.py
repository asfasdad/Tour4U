import importlib

import pandas as pd


data_diff = importlib.import_module("data_diff")


def test_normalize_url_drops_query_and_fragment():
    url = "HTTPS://Example.com/path/item/?a=1#part"
    assert data_diff.normalize_url(url) == "https://example.com/path/item"


def test_sku_fingerprint_is_stable_for_query_changes():
    a = data_diff.sku_fingerprint("https://example.com/products/abc?utm=1")
    b = data_diff.sku_fingerprint("https://example.com/products/abc?utm=2")
    assert a == b
    assert len(a) == 64


def test_infer_price_from_text_supports_common_symbols():
    assert data_diff.infer_price_from_text("Now only $1,299.50") == 1299.5
    assert data_diff.infer_price_from_text("Price 88€") == 88.0
    assert data_diff.infer_price_from_text("Only £79.99 today") == 79.99
    assert data_diff.infer_price_from_text("USD 249.00") == 249.0
    assert data_diff.infer_price_from_text("N/A") is None


def test_batch_diff_detects_new_removed_and_modified():
    prev = pd.DataFrame(
        [
            {"Final URL": "https://a.com/p1", "Price": 10, "Review Count": 1, "Domain": "a.com"},
            {"Final URL": "https://a.com/p2", "Price": 20, "Review Count": 5, "Domain": "a.com"},
        ]
    )
    curr = pd.DataFrame(
        [
            {"Final URL": "https://a.com/p1", "Price": 12, "Review Count": 2, "Domain": "a.com"},
            {"Final URL": "https://a.com/p3", "Price": 9, "Review Count": 1, "Domain": "a.com"},
        ]
    )

    result = data_diff.batch_diff(prev, curr)
    summary = result["summary"]

    assert summary["new"] == 1
    assert summary["removed"] == 1
    assert summary["modified"] == 1
    assert summary["price_up"] == 1
    assert summary["review_growing"] == 1


def test_batch_diff_handles_missing_url_columns_without_crash():
    prev = pd.DataFrame([{"Price": 10, "Review Count": 1, "Domain": "a.com"}])
    curr = pd.DataFrame([{"Price": 12, "Review Count": 2, "Domain": "a.com"}])

    result = data_diff.batch_diff(prev, curr)
    summary = result["summary"]

    assert summary["prev_count"] == 1
    assert summary["curr_count"] == 1
    assert isinstance(result["items"], list)


def test_compute_diff_with_composite_key_and_numeric_delta():
    prev = pd.DataFrame(
        [
            {"Product": "A", "Domain": "x.com", "Price": 10.0},
            {"Product": "B", "Domain": "x.com", "Price": 20.0},
        ]
    )
    curr = pd.DataFrame(
        [
            {"Product": "A", "Domain": "x.com", "Price": 12.0},
            {"Product": "C", "Domain": "x.com", "Price": 30.0},
        ]
    )

    result = data_diff.compute_diff(prev, curr)
    assert result["added_count"] == 1
    assert result["removed_count"] == 1
    assert "Price" in result["numeric_deltas"]


def test_get_diff_summary_for_ui_truncates_ids_to_100():
    diff_result = {
        "row_count_prev": 10,
        "row_count_curr": 20,
        "added_count": 150,
        "removed_count": 0,
        "added_ids": {f"id-{i}" for i in range(150)},
        "removed_ids": set(),
        "numeric_deltas": {},
        "id_column": "sku_id",
    }
    summary = data_diff.get_diff_summary_for_ui(diff_result)
    assert summary["row_delta"] == 10
    assert len(summary["added_ids"]) == 100


def test_compute_diff_handles_empty_previous_dataframe():
    curr = pd.DataFrame([{"SKU": "x1", "Price": 1.0}, {"SKU": "x2", "Price": 2.0}])
    result = data_diff.compute_diff(None, curr, id_column="SKU")
    assert result["row_count_prev"] == 0
    assert result["row_count_curr"] == 2
    assert result["added_count"] == 2
