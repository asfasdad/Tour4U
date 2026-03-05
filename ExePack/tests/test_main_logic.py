import ast
import hashlib
import json
import os
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import urlparse

import pandas as pd

from data_diff import sku_fingerprint


def _load_main_functions(names, extra_globals=None):
    source = Path(__file__).resolve().parents[1].joinpath("main.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    selected = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in set(names)]
    module = ast.Module(body=selected, type_ignores=[])
    code = compile(module, filename="main.py", mode="exec")
    namespace = {
        "Any": Any,
        "hashlib": hashlib,
        "json": json,
        "os": os,
        "pd": pd,
        "re": re,
        "sku_fingerprint": sku_fingerprint,
        "urlparse": urlparse,
    }
    if extra_globals:
        namespace.update(extra_globals)
    exec(code, namespace)
    return {name: namespace[name] for name in names}


def test_url_and_query_helpers_work_as_expected():
    funcs = _load_main_functions(
        [
            "_url_md5",
            "_get_query_param",
            "_extract_campaign_id_from_url",
            "_extract_adgroup_id_from_url",
        ]
    )
    assert funcs["_url_md5"]("https://example.com") == hashlib.md5("https://example.com".encode("utf-8")).hexdigest()

    url = "https://x.com/?CampaignID=abc&adgroup_id=grp-01"
    assert funcs["_get_query_param"](url, ["campaignid"]) == "abc"
    assert funcs["_extract_campaign_id_from_url"](url) == "abc"
    assert funcs["_extract_adgroup_id_from_url"](url) == "grp-01"


def test_classify_landing_page_categories():
    classify_landing_page = _load_main_functions(["classify_landing_page"])["classify_landing_page"]
    assert classify_landing_page("https://a.com/") == "首页 (Home)"
    assert classify_landing_page("https://a.com/products/p1") == "单品直连 (PDP)"
    assert classify_landing_page("https://a.com/collections/c1") == "集合页 (Collection)"
    assert classify_landing_page("https://a.com/blog/post") == "Other"


def test_parse_shopify_product_extracts_price_currency_and_stock():
    parse_shopify_product = _load_main_functions(["parse_shopify_product"])["parse_shopify_product"]
    html = (
        "<meta property='og:availability' content='instock'/>"
        "<script type='application/ld+json'>{\"offers\":{\"price\":\"19.99\",\"priceCurrency\":\"USD\"}}</script>"
    )
    parsed = parse_shopify_product(html)
    assert parsed["price"] == 19.99
    assert parsed["currency"] == "USD"
    assert parsed["compare_at_price"] is None
    assert parsed["is_available"] == 1


def test_infer_campaign_and_enrich_dataframe_for_ui():
    funcs = _load_main_functions(
        [
            "_get_query_param",
            "_extract_campaign_id_from_url",
            "_infer_campaign_and_batch_from_filename",
            "enrich_dataframe_for_ui",
        ]
    )
    campaign, batch = funcs["_infer_campaign_and_batch_from_filename"]("20260224_193000_springsale_summary.csv")
    assert campaign == "springsale"
    assert batch == "20260224_193000"

    df = pd.DataFrame(
        [
            {
                "Domain": "shop.example.com",
                "Final URL": "https://shop.example.com/products/tent?gad_campaignid=cmp123",
            }
        ]
    )
    out = funcs["enrich_dataframe_for_ui"](df, "20260224_193000_springsale_summary.csv")
    assert out.loc[0, "Campaign"] == "springsale"
    assert out.loc[0, "Batch ID"] == "20260224_193000"
    assert out.loc[0, "Campaign ID"] == "cmp123"
    assert out.loc[0, "Brand"] == "Example"
    assert out.loc[0, "Page Type"] == "Product Page (详情页)"
    assert out.loc[0, "parse_strategy"] == ""
    assert isinstance(out.loc[0, "sku_id"], str) and len(out.loc[0, "sku_id"]) == 64


def test_enrich_dataframe_backfills_blank_existing_columns():
    funcs = _load_main_functions(
        [
            "_get_query_param",
            "_extract_campaign_id_from_url",
            "_infer_campaign_and_batch_from_filename",
            "enrich_dataframe_for_ui",
        ]
    )
    df = pd.DataFrame(
        [
            {
                "Campaign": "",
                "Batch ID": "",
                "Campaign ID": "",
                "Brand": "",
                "Page Type": "",
                "Domain": "shop.example.com",
                "Final URL": "https://shop.example.com/products/tent?gad_campaignid=cmp999",
            }
        ]
    )
    out = funcs["enrich_dataframe_for_ui"](df, "20260224_193000_springsale_summary.csv")
    assert out.loc[0, "Campaign"] == "springsale"
    assert out.loc[0, "Batch ID"] == "20260224_193000"
    assert out.loc[0, "Campaign ID"] == "cmp999"
    assert out.loc[0, "Brand"] == "Example"
    assert out.loc[0, "Page Type"] == "Product Page (详情页)"


def test_adparser_enrich_and_dedup_computes_occurrence_count():
    adparser_enrich_and_dedup = _load_main_functions(
        ["_get_query_param", "adparser_enrich_and_dedup"]
    )["adparser_enrich_and_dedup"]
    df = pd.DataFrame(
        [
            {
                "Final URL": "https://a.com/p/1?gad_campaignid=c1&adgroupid=g1",
                "Product": "Tent X",
                "Timestamp": "2026-02-24 10:00",
            },
            {
                "Final URL": "https://a.com/p/1?gad_campaignid=c1&adgroupid=g1",
                "Product": "Tent X",
                "Timestamp": "2026-02-24 11:00",
            },
        ]
    )
    enriched, dedup = adparser_enrich_and_dedup(df)
    assert int(enriched["出现次数"].max()) == 2
    assert len(dedup) == 1
    assert int(dedup.iloc[0]["出现次数"]) == 2


def test_ensure_domain_and_product_id_fills_missing_fields():
    ensure_domain_and_product_id = _load_main_functions(["_url_md5", "ensure_domain_and_product_id"])[
        "ensure_domain_and_product_id"
    ]
    df = pd.DataFrame([{"Final URL": "https://brand.com/products/a"}])
    out = ensure_domain_and_product_id(df)
    assert out.loc[0, "domain"] == "brand.com"
    assert isinstance(out.loc[0, "product_id"], str)
    assert len(out.loc[0, "product_id"]) == 32


def test_select_prev_history_filename_prefers_same_campaign_and_previous_time():
    select_prev = _load_main_functions(["_select_prev_history_filename"])["_select_prev_history_filename"]
    files = [
        "20260206_140311_tent.csv",
        "20260206_141024_naturehike tent.csv",
        "20260206_141327_naturehike tent.csv",
        "20260206_145736_naturehike tent.csv",
    ]
    prev_name = select_prev("20260206_145736_naturehike tent.csv", files)
    assert prev_name == "20260206_141327_naturehike tent.csv"


def test_select_prev_history_filename_fallback_to_latest_earlier_when_campaign_differs():
    select_prev = _load_main_functions(["_select_prev_history_filename"])["_select_prev_history_filename"]
    files = [
        "20260206_110410_ultralight tent.csv",
        "20260206_112051_ultralight tent.csv",
        "20260206_130434_camping chair.csv",
    ]
    prev_name = select_prev("20260206_130434_camping chair.csv", files)
    assert prev_name == "20260206_112051_ultralight tent.csv"


def test_get_unified_filter_options_extracts_domain_keyword_product():
    funcs = _load_main_functions(["get_unified_filter_options"])
    get_opts = funcs["get_unified_filter_options"]
    df = pd.DataFrame(
        [
            {"Domain": "b.com", "Keyword": "k2", "Product": "p2"},
            {"Domain": "a.com", "Keyword": "k1", "Product": "p1"},
        ]
    )
    opts = get_opts(df)
    assert opts["domains"] == ["a.com", "b.com"]
    assert opts["keywords"] == ["k1", "k2"]
    assert opts["products"] == ["p1", "p2"]


def test_build_unified_analysis_data_filters_and_builds_trend():
    funcs = _load_main_functions(["build_unified_analysis_data"])
    build_data = funcs["build_unified_analysis_data"]

    df = pd.DataFrame(
        [
            {
                "Domain": "x.com",
                "Keyword": "tent",
                "Product": "T1",
                "Timestamp": "2026-02-20 10:00",
                "Price": 10,
                "Review Count": 1,
            },
            {
                "Domain": "x.com",
                "Keyword": "tent",
                "Product": "T1",
                "Timestamp": "2026-02-21 10:00",
                "Price": 12,
                "Review Count": 2,
            },
            {
                "Domain": "y.com",
                "Keyword": "chair",
                "Product": "C1",
                "Timestamp": "2026-02-21 11:00",
                "Price": 8,
                "Review Count": 4,
            },
        ]
    )

    result = build_data(df, domain="x.com", keyword="tent", product="T1")
    assert len(result["filtered"]) == 2
    assert int(result["summary"]["records"]) == 2
    assert int(result["summary"]["unique_products"]) == 1
    trend = result["trend"]
    assert len(trend) == 2
    assert "avg_price" in trend.columns


def test_parse_blocked_domains_and_domain_match():
    funcs = _load_main_functions(["_parse_blocked_domains", "_is_blocked_domain"])
    parse_blocked = funcs["_parse_blocked_domains"]
    is_blocked = funcs["_is_blocked_domain"]

    blocked = parse_blocked(" https://my.com,shop.my.com ; test.com\nmy.com ")
    assert blocked == ["my.com", "shop.my.com", "test.com"]
    assert is_blocked("shop.my.com", blocked) is True
    assert is_blocked("a.shop.my.com", blocked) is True
    assert is_blocked("other.com", blocked) is False


def test_extract_review_count_from_html_prefers_json_ld():
    extract_reviews = _load_main_functions(["_extract_review_count_from_html"])["_extract_review_count_from_html"]
    html = """
    <script type='application/ld+json'>
    {"aggregateRating":{"reviewCount":"1234","ratingValue":"4.7"}}
    </script>
    <div>8 reviews</div>
    """
    assert extract_reviews(html) == 1234


def test_extract_review_count_from_html_regex_fallback():
    extract_reviews = _load_main_functions(["_extract_review_count_from_html"])["_extract_review_count_from_html"]
    assert extract_reviews("This item has 89 reviews and counting") == 89
    assert extract_reviews("Rated by 456 ratings") == 456


def test_compute_adgroup_changes_returns_daily_deltas():
    compute_changes = _load_main_functions(["compute_adgroup_changes"])["compute_adgroup_changes"]
    df = pd.DataFrame(
        [
            {"Timestamp": "2026-02-24 10:00", "广告组ID": "g1"},
            {"Timestamp": "2026-02-24 11:00", "广告组ID": "g2"},
            {"Timestamp": "2026-02-25 10:00", "广告组ID": "g2"},
            {"Timestamp": "2026-02-25 11:00", "广告组ID": "g3"},
        ]
    )
    out = compute_changes(df)
    assert len(out) == 2
    assert int(out.iloc[0]["adgroup_count"]) == 2
    assert int(out.iloc[1]["new_adgroups"]) == 1
    assert int(out.iloc[1]["removed_adgroups"]) == 1


def test_minimal_e2e_flow_with_mocked_url_resolution():
    fake_requests = SimpleNamespace(get=lambda *args, **kwargs: SimpleNamespace(url="https://dest.com/products/z"))
    funcs = _load_main_functions(
        [
            "_url_md5",
            "resolve_url_fast",
            "_get_query_param",
            "_extract_campaign_id_from_url",
            "_infer_campaign_and_batch_from_filename",
            "enrich_dataframe_for_ui",
            "adparser_enrich_and_dedup",
            "ensure_domain_and_product_id",
        ],
        extra_globals={"requests": fake_requests},
    )

    raw = "https://www.google.com/aclk?adurl=https://dest.com/products/z?gad_campaignid=run123"
    final_url = funcs["resolve_url_fast"](raw)
    assert final_url.startswith("https://dest.com/products/z")

    input_df = pd.DataFrame(
        [{"Final URL": final_url, "Domain": "dest.com", "Product": "Z Tent", "Timestamp": "2026-02-24 19:00"}]
    )
    stage1 = funcs["ensure_domain_and_product_id"](input_df)
    stage2 = funcs["enrich_dataframe_for_ui"](stage1, "20260224_190000_campaign_demo.csv")
    enriched, dedup = funcs["adparser_enrich_and_dedup"](stage2)

    assert len(enriched) == 1
    assert len(dedup) == 1
    assert stage2.loc[0, "Campaign ID"] == "run123"
    assert stage2.loc[0, "Campaign"] == "campaign"


def test_append_dedup_helpers_generate_stable_keys(tmp_path):
    funcs = _load_main_functions(
        [
            "_normalize_url_for_dedup",
            "_ad_row_unique_key",
            "_collect_existing_row_keys",
        ]
    )

    csv_path = tmp_path / "append_case.csv"
    pd.DataFrame(
        [
            {
                "Final URL": "https://A.com/p/1?b=2&a=1#frag",
                "Campaign ID": "C1",
            },
            {
                "Final URL": "https://a.com/p/1?a=1&b=2",
                "Campaign ID": "c1",
            },
            {
                "Final URL": "",
                "Campaign ID": "C2",
            },
        ]
    ).to_csv(csv_path, index=False, encoding="utf-8-sig")

    keys = funcs["_collect_existing_row_keys"](str(csv_path))

    url_key = funcs["_ad_row_unique_key"]("https://a.com/p/1?a=1&b=2", "c1")
    cid_key = funcs["_ad_row_unique_key"]("", "C2")

    assert url_key in keys
    assert cid_key in keys
    assert len(keys) == 2


def test_delete_history_file_removes_csv_and_calls_session_cleanup(tmp_path):
    called = {"name": None}

    def _fake_delete_session_file_key(name):
        called["name"] = name
        return True

    funcs = _load_main_functions(
        ["delete_history_file"],
        extra_globals={
            "HISTORY_DIR": str(tmp_path),
            "delete_session_file_key": _fake_delete_session_file_key,
        },
    )

    file_name = "to_delete.csv"
    p = tmp_path / file_name
    p.write_text("a,b\n1,2\n", encoding="utf-8")

    ok = funcs["delete_history_file"](file_name)
    assert ok is True
    assert not p.exists()
    assert called["name"] == file_name

    ok_missing = funcs["delete_history_file"]("missing.csv")
    assert ok_missing is False


def test_build_adgroup_change_rebuild_data_returns_drilldown_details():
    funcs = _load_main_functions(["build_adgroup_change_rebuild_data"])
    build_data = funcs["build_adgroup_change_rebuild_data"]

    df = pd.DataFrame(
        [
            {
                "Timestamp": "2026-03-01 10:00",
                "Domain": "a.com",
                "product_name": "P1",
                "广告组ID": "G1",
                "Final URL": "https://a.com/p1",
            },
            {
                "Timestamp": "2026-03-02 10:00",
                "Domain": "a.com",
                "product_name": "P1",
                "广告组ID": "G2",
                "Final URL": "https://a.com/p2",
            },
        ]
    )

    out = build_data(df, mode="产品")
    assert not out["summary"].empty
    assert not out["details"].empty
    assert set(out["details"]["change_type"].unique().tolist()) >= {"新增"}


def test_build_keyword_insight_data_dedups_rows_and_counts_occurrence():
    funcs = _load_main_functions(["_normalize_url_for_dedup", "build_keyword_insight_data"])
    build_data = funcs["build_keyword_insight_data"]

    df = pd.DataFrame(
        [
            {
                "Timestamp": "2026-03-01 10:00",
                "Domain": "a.com",
                "Keyword": "chair",
                "Product": "P1",
                "Final URL": "https://a.com/p1?b=2&a=1",
                "Price": "10",
                "Review Count": "1",
            },
            {
                "Timestamp": "2026-03-01 11:00",
                "Domain": "a.com",
                "Keyword": "chair",
                "Product": "P1",
                "Final URL": "https://a.com/p1?a=1&b=2",
                "Price": "11",
                "Review Count": "2",
            },
        ]
    )

    out = build_data(df, keyword="chair", granularity="小时")
    assert int(out["summary"]["records"]) == 2
    assert int(out["summary"]["unique_rows"]) == 1
    assert int(out["details"].iloc[0]["出现次数"]) == 2
