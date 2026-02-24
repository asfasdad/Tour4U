import ast
import hashlib
import json
import os
import re
from pathlib import Path
from types import SimpleNamespace
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
