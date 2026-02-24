# -*- coding: utf-8 -*-
"""
竞品分析工具 v15 — 核心架构重构版
- 多轮会话链 (Session + HistoryItem)
- 生产者-消费者爬虫 + 实时看板
- 自动持久化 & 广告组对比
- 异步报告生成
"""
import streamlit as st
import pandas as pd
from DrissionPage import ChromiumPage, ChromiumOptions
from urllib.parse import urlparse
import sys
import time
import random
import requests
import threading
import queue
import csv
import io
from datetime import datetime
import os
import re
import sqlite3
import hashlib
import json
import altair as alt
import socket
import subprocess
from typing import Any

from llm_providers import get_provider, PLATFORM_MODELS, DEFAULT_BASE_URLS
from session_store import (
    get_session_for_file,
    save_session_for_file,
    list_history_files_with_sessions,
    rename_session_file_key,
)
from data_diff import compute_diff, make_snapshot, get_diff_summary_for_ui, sku_fingerprint, infer_price_from_text

# --- 常量（内置配置，不暴露在 UI）---
if getattr(sys, "frozen", False):
    APP_BASE_DIR = os.path.dirname(sys.executable)
else:
    APP_BASE_DIR = os.path.dirname(os.path.abspath(__file__))

HISTORY_DIR = os.path.join(APP_BASE_DIR, "history_data")
DEFAULT_TARGET_URL = "https://www.google.com"
if not os.path.exists(HISTORY_DIR):
    os.makedirs(HISTORY_DIR)

DB_PATH = os.path.join(HISTORY_DIR, "analysis.sqlite3")
DB_LOCK = threading.Lock()


@st.cache_resource
def _get_engine_resources():
    lock = threading.Lock()
    state = {
        "running": False,
        "stop_requested": False,
        "error": None,
        "done": 0,
        "fail": 0,
        "pending": 0,
        "discovered": 0,
        "save_filename": "",
        "shared_list": None,
        "transferred": False,
    }
    return lock, state


ENGINE_LOCK, ENGINE_STATE = _get_engine_resources()


def _url_md5(url: str) -> str:
    return hashlib.md5((url or "").encode("utf-8")).hexdigest()


def _get_query_param(url: str, keys: list[str]) -> str:
    try:
        from urllib.parse import parse_qs, urlparse

        qs = parse_qs(urlparse(url).query)
        for key in keys:
            for k, v in qs.items():
                if k.lower() == key.lower() and v:
                    return str(v[0]).strip()
    except Exception:
        pass
    return ""


def _extract_campaign_id_from_url(url: str) -> str:
    return _get_query_param(url, ["campaignid", "gad_campaignid", "gbraid"])


def _extract_adgroup_id_from_url(url: str) -> str:
    return _get_query_param(url, ["adgroupid", "adgroup_id", "adset_id", "adsetid"])


def _parse_blocked_domains(text: str) -> list[str]:
    raw = str(text or "")
    parts = [p.strip().lower() for p in re.split(r"[,;\n\s]+", raw) if p and p.strip()]
    out: list[str] = []
    for p in parts:
        p = p.replace("https://", "").replace("http://", "").strip("/")
        if p and p not in out:
            out.append(p)
    return out


def _is_blocked_domain(domain: str, blocked_domains: list[str]) -> bool:
    d = str(domain or "").lower().split(":")[0].strip()
    if not d:
        return False
    for b in blocked_domains or []:
        bb = str(b or "").lower().split(":")[0].strip()
        if not bb:
            continue
        if d == bb or d.endswith("." + bb):
            return True
    return False


def _extract_review_count_from_html(html: str) -> int | None:
    content = html or ""

    def _to_int(v) -> int | None:
        try:
            if v is None:
                return None
            s = str(v).strip().replace(",", "")
            if not s:
                return None
            n = int(float(s))
            return n if n >= 0 else None
        except Exception:
            return None

    # JSON-LD 优先
    try:
        scripts = re.findall(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', content, flags=re.I | re.S)
        for raw in scripts:
            raw = raw.strip()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue
            objs = data if isinstance(data, list) else [data]
            for obj in objs:
                if not isinstance(obj, dict):
                    continue
                agg = obj.get("aggregateRating")
                if isinstance(agg, dict):
                    n = _to_int(agg.get("reviewCount") or agg.get("ratingCount"))
                    if n is not None:
                        return n
                n = _to_int(obj.get("reviewCount") or obj.get("ratingCount"))
                if n is not None:
                    return n
    except Exception:
        pass

    # 文本兜底
    for pat in [
        r"([\d,]{1,12})\s*reviews?\b",
        r"([\d,]{1,12})\s*ratings?\b",
        r"reviewCount\s*[:=]\s*[\"']?([\d,]{1,12})",
        r"([\d,]{1,12})\s*条?评论",
        r"([\d,]{1,12})\s*评",
    ]:
        m = re.search(pat, content, re.I)
        if m:
            n = _to_int(m.group(1))
            if n is not None:
                return n
    return None


def _find_free_port(preferred_range: tuple[int, int] = (20000, 40000)) -> int:
    start, end = preferred_range
    for _ in range(60):
        port = random.randint(start, end)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    # fallback: let OS pick
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _engine_thread_run(k_list: list[str], settings: dict, current_save_path: str, current_save_filename: str) -> None:
    """后台线程运行引擎。不要在此函数里调用任何 st.* API。"""
    try:
        with ENGINE_LOCK:
            ENGINE_STATE.update(
                {
                    "running": True,
                    "stop_requested": False,
                    "error": None,
                    "done": 0,
                    "fail": 0,
                    "pending": 0,
                    "discovered": 0,
                    "save_filename": current_save_filename,
                    "shared_list": [],
                    "transferred": False,
                }
            )

        co = ChromiumOptions()
        co.set_argument("--no-first-run")
        # profile
        try:
            profile_dir = os.path.join(HISTORY_DIR, "chrome_profiles", settings.get("batch_id", "run"))
            os.makedirs(profile_dir, exist_ok=True)
            co.set_argument(f"--user-data-dir={profile_dir}")
        except Exception:
            pass
        # local port
        try:
            co.set_local_port(_find_free_port((20000, 40000)))
        except Exception:
            pass
        if settings.get("force_headless"):
            co.set_argument("--headless=new")
        else:
            co.headless(False)
        co.mute(True)
        if settings.get("proxy"):
            co.set_proxy(settings.get("proxy"))

        # browser
        try:
            browser = ChromiumPage(co)
        except Exception as e:
            # proxy fallback
            if settings.get("proxy") and "Handshake status 404" in str(e):
                co2 = ChromiumOptions()
                co2.set_argument("--no-first-run")
                try:
                    profile_dir = os.path.join(HISTORY_DIR, "chrome_profiles", settings.get("batch_id", "run") + "_noproxy")
                    os.makedirs(profile_dir, exist_ok=True)
                    co2.set_argument(f"--user-data-dir={profile_dir}")
                except Exception:
                    pass
                try:
                    co2.set_local_port(_find_free_port((20000, 40000)))
                except Exception:
                    pass
                if settings.get("force_headless"):
                    co2.set_argument("--headless=new")
                else:
                    co2.headless(False)
                co2.mute(True)
                browser = ChromiumPage(co2)
            else:
                raise

        link_queue = queue.Queue()
        total_discovered = [0]
        lock = threading.Lock()
        counters = {"done": 0, "fail": 0}
        is_running_ref = [True]

        # producer in this thread (DrissionPage must be single-thread; background is ok)
        # NOTE: we cannot use st.session_state here; use ENGINE_STATE.stop_requested
        base = DEFAULT_TARGET_URL.rstrip("/")
        for keyword in k_list:
            with ENGINE_LOCK:
                if ENGINE_STATE.get("stop_requested"):
                    break
            tab = None
            try:
                tab = browser.new_tab()
                gl, hl = settings.get("gl", "us"), settings.get("hl", "en")
                for i in range(int(settings.get("pages", 1))):
                    with ENGINE_LOCK:
                        if ENGINE_STATE.get("stop_requested"):
                            break
                    search_url = f"{base}/search?q={keyword}&start={i*10}&gl={gl}&hl={hl}&pws=0"
                    try:
                        tab.get(search_url, timeout=25)
                        time.sleep(2)
                        tab.scroll.to_bottom()
                        time.sleep(1)
                        tab.scroll.to_top()
                        time.sleep(0.5)
                    except Exception:
                        pass
                    try:
                        links = tab.eles("xpath://a[@href]")
                        for link in links:
                            with ENGINE_LOCK:
                                if ENGINE_STATE.get("stop_requested"):
                                    break
                            try:
                                href = link.attr("href")
                                if href and "/aclk" in href:
                                    link_queue.put((keyword, href))
                                    total_discovered[0] += 1
                            except Exception:
                                continue
                    except Exception:
                        pass
                    with ENGINE_LOCK:
                        ENGINE_STATE["discovered"] = total_discovered[0]
                        ENGINE_STATE["pending"] = link_queue.qsize()
                    time.sleep(random.uniform(1, 2))
            finally:
                if tab:
                    try:
                        tab.close()
                    except Exception:
                        pass

        # end signals
        for _ in range(int(settings.get("num_workers", 2))):
            link_queue.put(None)

        # consumers
        workers = []
        for _ in range(int(settings.get("num_workers", 2))):
            t = threading.Thread(
                target=consumer_worker,
                args=(link_queue, ENGINE_STATE["shared_list"], lock, counters, is_running_ref, settings, current_save_path),
                daemon=True,
            )
            t.start()
            workers.append(t)

        while True:
            stop_now = False
            with ENGINE_LOCK:
                if ENGINE_STATE.get("stop_requested"):
                    is_running_ref[0] = False
                    stop_now = True
            if stop_now:
                # 立即清空队列，避免卡在大量 pending
                try:
                    while True:
                        link_queue.get_nowait()
                except Exception:
                    pass
                for _ in range(int(settings.get("num_workers", 2))):
                    try:
                        link_queue.put_nowait(None)
                    except Exception:
                        pass
                # 等待一小段时间让消费者退出
                for t in workers:
                    try:
                        t.join(timeout=3)
                    except Exception:
                        pass
                break

            with ENGINE_LOCK:
                ENGINE_STATE["done"] = counters["done"]
                ENGINE_STATE["fail"] = counters["fail"]
                ENGINE_STATE["pending"] = link_queue.qsize()
                ENGINE_STATE["discovered"] = total_discovered[0]
            if link_queue.qsize() == 0 and not any(t.is_alive() for t in workers):
                break
            time.sleep(0.4)

        try:
            browser.quit()
        except Exception:
            pass

    except Exception as e:
        with ENGINE_LOCK:
            ENGINE_STATE["error"] = str(e)
    finally:
        with ENGINE_LOCK:
            ENGINE_STATE["running"] = False


def _get_listening_pids_on_port(port: int) -> list[int]:
    """Windows: parse netstat to find LISTENING PIDs on a port."""
    try:
        out = subprocess.check_output(["netstat", "-ano"], text=True, errors="ignore")
    except Exception:
        return []
    pids: set[int] = set()
    needle = f":{int(port)}"
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        if needle not in line:
            continue
        if "LISTENING" not in line.upper():
            continue
        parts = line.split()
        if len(parts) >= 5:
            try:
                pids.add(int(parts[-1]))
            except Exception:
                pass
    return sorted(pids)


def _db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
    return {r[1] for r in rows}


def init_sqlite_schema() -> None:
    """创建/更新三张表：ad_impressions, products, product_states。"""
    with DB_LOCK:
        conn = _db_connect()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ad_impressions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    observed_at TEXT,
                    keyword TEXT,
                    gad_campaignid TEXT,
                    url TEXT,
                    raw_url TEXT,
                    domain TEXT,
                    brand TEXT,
                    ad_type TEXT,
                    ad_signals TEXT,
                    product_id TEXT,
                    batch_id TEXT,
                    campaign TEXT
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS products (
                    id TEXT PRIMARY KEY,
                    domain TEXT,
                    title TEXT,
                    handle TEXT,
                    image_url TEXT,
                    first_seen_at TEXT
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS product_states (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_id TEXT,
                    observed_at TEXT,
                    price REAL,
                    compare_at_price REAL,
                    is_available INTEGER,
                    currency TEXT,
                    FOREIGN KEY(product_id) REFERENCES products(id)
                );
                """
            )

            # 轻量“存在则更新”：补齐可能缺失的列
            cols = _table_columns(conn, "ad_impressions")
            for col, ddl in {
                "raw_url": "ALTER TABLE ad_impressions ADD COLUMN raw_url TEXT;",
                "product_id": "ALTER TABLE ad_impressions ADD COLUMN product_id TEXT;",
                "batch_id": "ALTER TABLE ad_impressions ADD COLUMN batch_id TEXT;",
                "campaign": "ALTER TABLE ad_impressions ADD COLUMN campaign TEXT;",
            }.items():
                if col not in cols:
                    conn.execute(ddl)

            cols2 = _table_columns(conn, "product_states")
            for col, ddl in {
                "compare_at_price": "ALTER TABLE product_states ADD COLUMN compare_at_price REAL;",
                "currency": "ALTER TABLE product_states ADD COLUMN currency TEXT;",
            }.items():
                if col not in cols2:
                    conn.execute(ddl)

            cols3 = _table_columns(conn, "products")
            if "image_url" not in cols3:
                conn.execute("ALTER TABLE products ADD COLUMN image_url TEXT;")

            conn.commit()
        finally:
            conn.close()


def load_sqlite_table_as_df(query: str) -> pd.DataFrame:
    with DB_LOCK:
        conn = _db_connect()
        try:
            return pd.read_sql_query(query, conn)
        finally:
            conn.close()


def sql_df(query: str, params: tuple | list | None = None) -> pd.DataFrame:
    with DB_LOCK:
        conn = _db_connect()
        try:
            return pd.read_sql_query(query, conn, params=params)
        finally:
            conn.close()


def get_selected_domain() -> str:
    return str(st.session_state.get("selected_domain") or "").strip()


def get_daily_trend(product_id: str, domain: str) -> pd.DataFrame:
    if not product_id or not domain:
        return pd.DataFrame()

    states = sql_df(
        """
        SELECT observed_at, price, is_available
        FROM product_states
        WHERE product_id = ?
        ORDER BY observed_at
        """,
        params=(product_id,),
    )
    if states is None or states.empty:
        states = pd.DataFrame(columns=["observed_at", "price", "is_available"])
    states["observed_at"] = pd.to_datetime(states["observed_at"], errors="coerce")
    states = states.dropna(subset=["observed_at"])
    states["date"] = states["observed_at"].dt.date
    states["price"] = pd.to_numeric(states["price"], errors="coerce")

    if not states.empty:
        price_daily = (
            states.groupby("date")["price"]
            .agg(price_min="min", price_max="max")
            .reset_index()
        )
        stock_daily = (
            states.sort_values("observed_at")
            .groupby("date")["is_available"]
            .last()
            .reset_index()
            .rename(columns={"is_available": "stock_last"})
        )
        out = price_daily.merge(stock_daily, on="date", how="outer")
    else:
        out = pd.DataFrame(columns=["date", "price_min", "price_max", "stock_last"])

    ads = sql_df(
        """
        SELECT observed_at, batch_id
        FROM ad_impressions
        WHERE product_id = ? AND domain = ?
        ORDER BY observed_at
        """,
        params=(product_id, domain),
    )
    if ads is None or ads.empty:
        ads = pd.DataFrame(columns=["observed_at", "batch_id"])
    ads["observed_at"] = pd.to_datetime(ads["observed_at"], errors="coerce")
    ads = ads.dropna(subset=["observed_at"])
    if not ads.empty:
        ads["date"] = ads["observed_at"].dt.date
        by_batch = ads.groupby(["date", "batch_id"]).size().reset_index(name="ad_cnt")
        ad_daily = by_batch.groupby("date")["ad_cnt"].max().reset_index(name="ad_count")
        out = out.merge(ad_daily, on="date", how="outer")
    else:
        out["ad_count"] = 0

    out = out.sort_values("date")
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date"])
    return out


def classify_landing_page(url: str) -> str:
    try:
        path = (urlparse(url).path or "").strip().lower()
        if path in {"", "/"}:
            return "首页 (Home)"
        if "/products/" in path:
            return "单品直连 (PDP)"
        if "/collections/" in path:
            return "集合页 (Collection)"
        return "Other"
    except Exception:
        return "Other"


# 初始化 DB（不影响 UI/爬虫主流程，失败也不阻断）
try:
    init_sqlite_schema()
except Exception as _e:
    print(f"SQLite init failed: {_e}")


def parse_shopify_product(html_content: str) -> dict:
    """解析 Shopify 商品页（尽量只用 JSON-LD + OG availability）。

    返回：{price, currency, compare_at_price, is_available, review_count}
    解析失败时返回空字段，不抛异常。
    """
    result = {"price": None, "currency": None, "compare_at_price": None, "is_available": None, "review_count": None}
    html = html_content or ""

    def _normalize_availability(val: str) -> int | None:
        if not val:
            return None
        v = str(val).strip().lower()
        v = v.replace("https://schema.org/", "").replace("http://schema.org/", "")
        if v in {"instock", "in stock", "in_stock"}:
            return 1
        if v in {"outofstock", "out of stock", "out_of_stock"}:
            return 0
        return None

    try:
        # OG availability
        m_og = re.search(r'<meta[^>]+property=["\']og:availability["\'][^>]+content=["\']([^"\']+)["\']', html, re.I)
        if m_og:
            result["is_available"] = _normalize_availability(m_og.group(1))
    except Exception:
        pass

    # JSON-LD offers
    try:
        scripts = re.findall(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, flags=re.I | re.S)
        for raw in scripts:
            raw = raw.strip()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue

            candidates = data if isinstance(data, list) else [data]
            for obj in candidates:
                if not isinstance(obj, dict):
                    continue
                offers = obj.get("offers")
                if isinstance(offers, list) and offers:
                    offers = offers[0]
                if isinstance(offers, dict):
                    if result["price"] is None:
                        p = offers.get("price")
                        if p is not None and str(p).strip() != "":
                            try:
                                result["price"] = float(p)
                            except Exception:
                                pass
                    if result["currency"] is None:
                        cur = offers.get("priceCurrency")
                        if cur:
                            result["currency"] = str(cur).strip()
                    if result["is_available"] is None:
                        avail = offers.get("availability") or obj.get("availability")
                        result["is_available"] = _normalize_availability(avail)
                # 评论数（JSON-LD 常见 aggregateRating.reviewCount）
                if result["review_count"] is None:
                    agg = obj.get("aggregateRating")
                    if isinstance(agg, dict):
                        val = agg.get("reviewCount") or agg.get("ratingCount")
                    else:
                        val = obj.get("reviewCount") or obj.get("ratingCount")
                    if val is not None and str(val).strip() != "":
                        try:
                            result["review_count"] = int(float(str(val).replace(",", "")))
                        except Exception:
                            pass
                # compare_at_price：JSON-LD 通常没有，留空
            if result["price"] is not None or result["is_available"] is not None:
                break
    except Exception:
        pass

    return result


def db_upsert_product_and_state(
    *,
    url: str,
    domain: str,
    title: str,
    image_url: str | None,
    observed_at: str,
    price: float | None,
    compare_at_price: float | None,
    is_available: int | None,
    currency: str | None,
) -> str:
    product_id = _url_md5(url)
    handle = ""
    try:
        p = urlparse(url).path or ""
        m = re.search(r"/products/([^/?#]+)", p, re.I)
        if m:
            handle = m.group(1)[:200]
    except Exception:
        pass

    with DB_LOCK:
        conn = _db_connect()
        try:
            conn.execute(
                "INSERT OR IGNORE INTO products (id, domain, title, handle, image_url, first_seen_at) VALUES (?, ?, ?, ?, ?, ?)",
                (product_id, domain, title, handle, image_url, observed_at),
            )
            # 若已存在则尽量补全可读字段
            conn.execute(
                """
                UPDATE products
                SET
                    domain = COALESCE(NULLIF(domain, ''), ?),
                    title = COALESCE(NULLIF(title, ''), ?),
                    handle = COALESCE(NULLIF(handle, ''), ?),
                    image_url = COALESCE(NULLIF(image_url, ''), ?)
                WHERE id = ?
                """,
                (domain, title, handle, image_url, product_id),
            )
            conn.execute(
                "INSERT INTO product_states (product_id, observed_at, price, compare_at_price, is_available, currency) VALUES (?, ?, ?, ?, ?, ?)",
                (product_id, observed_at, price, compare_at_price, is_available, currency),
            )
            conn.commit()
        finally:
            conn.close()
    return product_id


def db_insert_ad_impression(
    *,
    observed_at: str,
    keyword: str,
    gad_campaignid: str,
    url: str,
    raw_url: str,
    domain: str,
    brand: str,
    ad_type: str,
    ad_signals: str,
    product_id: str,
    batch_id: str,
    campaign: str,
) -> None:
    with DB_LOCK:
        conn = _db_connect()
        try:
            conn.execute(
                """
                INSERT INTO ad_impressions (
                    observed_at, keyword, gad_campaignid, url, raw_url, domain, brand, ad_type, ad_signals, product_id, batch_id, campaign
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    observed_at,
                    keyword,
                    gad_campaignid,
                    url,
                    raw_url,
                    domain,
                    brand,
                    ad_type,
                    ad_signals,
                    product_id,
                    batch_id,
                    campaign,
                ),
            )
            conn.commit()
        finally:
            conn.close()


def load_history_file(filename: str) -> pd.DataFrame | None:
    path = os.path.join(HISTORY_DIR, filename)
    if os.path.exists(path):
        for encoding in ("utf-8-sig", "utf-8", "gbk"):
            try:
                return pd.read_csv(path, encoding=encoding)
            except pd.errors.EmptyDataError:
                return pd.DataFrame()
            except UnicodeDecodeError:
                continue
            except Exception:
                continue
    return None


def create_empty_history_file(new_name: str) -> str | None:
    base_name = (new_name or "").strip()
    if not base_name:
        return None
    safe_name = "".join([c for c in base_name if c.isalnum() or c in (" ", "_", "-")]).strip()
    if not safe_name:
        return None
    if not safe_name.endswith(".csv"):
        safe_name += ".csv"
    path = os.path.join(HISTORY_DIR, safe_name)
    if os.path.exists(path):
        return None
    with open(path, "w", encoding="utf-8-sig", newline=""):
        pass
    return safe_name


def save_to_history(df: pd.DataFrame, keyword_summary: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_summary = "".join([c for c in keyword_summary if c.isalnum() or c in (" ", "_")]).strip()
    filename = f"{timestamp}_{safe_summary}.csv"
    path = os.path.join(HISTORY_DIR, filename)
    df.to_csv(path, index=False)
    return filename


def rename_history_file(old_name: str, new_name: str) -> bool:
    if not new_name.endswith(".csv"):
        new_name += ".csv"
    old_path = os.path.join(HISTORY_DIR, old_name)
    new_path = os.path.join(HISTORY_DIR, new_name)
    if os.path.exists(old_path) and not os.path.exists(new_path):
        os.rename(old_path, new_path)
        rename_session_file_key(old_name, new_name)
        return True
    return False


def _infer_campaign_and_batch_from_filename(filename: str) -> tuple[str, str]:
    """从历史文件名推断 batch_id 与 campaign 名（用于兼容旧历史）"""
    base = (filename or "").replace(".csv", "")
    parts = base.split("_")
    batch_id = ""
    campaign = ""
    if len(parts) >= 2 and parts[0].isdigit():
        batch_id = f"{parts[0]}_{parts[1]}"
        if len(parts) >= 3:
            # 新格式：{timestamp}_{campaign}_{summary}
            campaign = parts[2]
    return campaign, batch_id


def _select_prev_history_filename(current_filename: str, available_files: list[str]) -> str | None:
    """为当前历史文件选择一个“上一轮”文件名，用于 Batch Diff。"""
    if not current_filename or not available_files:
        return None

    def _parse_ts_and_campaign(name: str) -> tuple[str | None, str]:
        base = (name or "").replace(".csv", "")
        parts = base.split("_")
        if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
            ts = f"{parts[0]}_{parts[1]}"
            camp = parts[2] if len(parts) >= 3 else ""
            return ts, camp
        return None, ""

    cur_ts, cur_campaign = _parse_ts_and_campaign(current_filename)
    candidates = [f for f in available_files if f and f != current_filename]
    if not candidates:
        return None

    parsed = []
    for f in candidates:
        ts, camp = _parse_ts_and_campaign(f)
        if ts:
            parsed.append((f, ts, camp))

    if cur_ts:
        if cur_campaign:
            same_campaign = [(f, ts) for (f, ts, camp) in parsed if camp == cur_campaign and ts < cur_ts]
            if same_campaign:
                same_campaign.sort(key=lambda x: x[1], reverse=True)
                return same_campaign[0][0]

        earlier = [(f, ts) for (f, ts, _camp) in parsed if ts < cur_ts]
        if earlier:
            earlier.sort(key=lambda x: x[1], reverse=True)
            return earlier[0][0]

    parsed.sort(key=lambda x: x[1], reverse=True)
    return parsed[0][0] if parsed else None


def enrich_dataframe_for_ui(df: pd.DataFrame, history_filename: str = "") -> pd.DataFrame:
    """对旧历史数据做字段补全，确保新 UI（Domain->Campaign）能渲染。"""
    if df is None or df.empty:
        return df
    df = df.copy()

    inferred_campaign, inferred_batch_id = _infer_campaign_and_batch_from_filename(history_filename)

    def _fill_blank_with(series: pd.Series, fallback: str) -> pd.Series:
        if fallback is None:
            fallback = ""
        as_str = series.fillna("").astype(str)
        return as_str.where(as_str.str.strip() != "", fallback)

    if "Campaign" not in df.columns:
        df["Campaign"] = inferred_campaign
    else:
        df["Campaign"] = _fill_blank_with(df["Campaign"], inferred_campaign)
    if "Batch ID" not in df.columns:
        df["Batch ID"] = inferred_batch_id
    else:
        df["Batch ID"] = _fill_blank_with(df["Batch ID"], inferred_batch_id)

    if "Final URL" in df.columns:
        inferred_campaign_id = df["Final URL"].astype(str).apply(_extract_campaign_id_from_url)
    else:
        inferred_campaign_id = ""

    if "Campaign ID" not in df.columns:
        df["Campaign ID"] = inferred_campaign_id
    else:
        if "Final URL" in df.columns:
            as_str = df["Campaign ID"].fillna("").astype(str)
            inferred_str = inferred_campaign_id.astype(str)
            df["Campaign ID"] = as_str.where(as_str.str.strip() != "", inferred_str)
        else:
            df["Campaign ID"] = df["Campaign ID"].fillna("")

    if "Final URL" in df.columns and "sku_id" not in df.columns:
        df["sku_id"] = df["Final URL"].astype(str).apply(sku_fingerprint)

    if "Domain" in df.columns:
        def _brand_from_domain(domain: str) -> str:
            base = (domain or "").split(":")[0].lower()
            parts = base.split(".")
            core = parts[-2] if len(parts) >= 2 else base
            return (core[:1].upper() + core[1:]) if core else ""
        if "Brand" not in df.columns:
            df["Brand"] = df["Domain"].astype(str).apply(_brand_from_domain)
        else:
            inferred_brand = df["Domain"].astype(str).apply(_brand_from_domain)
            as_str = df["Brand"].fillna("").astype(str)
            df["Brand"] = as_str.where(as_str.str.strip() != "", inferred_brand)

    if "Final URL" in df.columns and "Page Type" not in df.columns:
        def _page_type(url: str) -> str:
            u = (url or "").lower()
            if any(x in u for x in ["/product/", "/products/", "/item/", "/p/"]):
                return "Product Page (详情页)"
            if any(x in u for x in ["/collection/", "/collections/", "/category/"]):
                return "Collection (列表页)"
            return "Other/Home"
        df["Page Type"] = df["Final URL"].astype(str).apply(_page_type)
    elif "Final URL" in df.columns and "Page Type" in df.columns:
        def _page_type(url: str) -> str:
            u = (url or "").lower()
            if any(x in u for x in ["/product/", "/products/", "/item/", "/p/"]):
                return "Product Page (详情页)"
            if any(x in u for x in ["/collection/", "/collections/", "/category/"]):
                return "Collection (列表页)"
            return "Other/Home"
        inferred_page_type = df["Final URL"].astype(str).apply(_page_type)
        as_str = df["Page Type"].fillna("").astype(str)
        df["Page Type"] = as_str.where(as_str.str.strip() != "", inferred_page_type)

    # 兼容旧记录：确保关键列存在
    if "parse_strategy" not in df.columns:
        df["parse_strategy"] = ""
    return df


def adparser_enrich_and_dedup(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """AdParser：解析广告字段并生成去重视图。

    - 主键：gad_campaignid（必须项，缺失则不参与去重）
    - 次要键：adgroupid、Brand、Product
    - 唯一标识：gad_campaignid + product_name + date
    """
    if df is None or df.empty:
        return df, df

    out = df.copy()

    if "Final URL" in out.columns:
        out["gad_campaignid"] = out["Final URL"].astype(str).apply(lambda u: _get_query_param(u, ["gad_campaignid"]))
        out["adgroupid"] = out["Final URL"].astype(str).apply(lambda u: _get_query_param(u, ["adgroupid", "adgroup_id"]))
    else:
        out["gad_campaignid"] = ""
        out["adgroupid"] = ""

    # 规范化 product_name
    if "Product" in out.columns:
        out["product_name"] = out["Product"].astype(str).fillna("").str.strip()
    else:
        out["product_name"] = ""

    # date
    if "Timestamp" in out.columns:
        out["date"] = pd.to_datetime(out["Timestamp"], errors="coerce").dt.strftime("%Y-%m-%d")
        out["date"] = out["date"].fillna("")
    else:
        out["date"] = ""

    # 唯一键（gad_campaignid 必须存在，否则不参与去重）
    out["ad_unique_key"] = out.apply(
        lambda r: f"{r.get('gad_campaignid','')}|{r.get('product_name','')}|{r.get('date','')}" if str(r.get("gad_campaignid", "")).strip() else "",
        axis=1,
    )

    # 只对具备主键的广告做去重统计
    has_key = out["ad_unique_key"].astype(str).str.len() > 0
    if has_key.any():
        counts = out.loc[has_key].groupby("ad_unique_key").size().rename("出现次数")
        out = out.merge(counts, left_on="ad_unique_key", right_index=True, how="left")
    else:
        out["出现次数"] = 1
    out["出现次数"] = out["出现次数"].fillna(1).astype(int)

    # 去重视图：每个 unique_key 只保留一条（无主键的保留原样，出现次数=1）
    dedup_parts = []
    if has_key.any():
        dedup_parts.append(out.loc[has_key].sort_values(["ad_unique_key"]).drop_duplicates("ad_unique_key", keep="first"))
    if (~has_key).any():
        tmp = out.loc[~has_key].copy()
        tmp["出现次数"] = 1
        dedup_parts.append(tmp)
    dedup_df = pd.concat(dedup_parts, ignore_index=True) if dedup_parts else out.copy()

    return out, dedup_df


def ensure_domain_and_product_id(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return df
    out = df.copy()
    if "domain" not in out.columns:
        if "Domain" in out.columns:
            out["domain"] = out["Domain"].astype(str)
        elif "Final URL" in out.columns:
            out["domain"] = out["Final URL"].astype(str).apply(lambda u: urlparse(u).netloc)
        else:
            out["domain"] = ""
    if "product_id" not in out.columns and "Final URL" in out.columns:
        out["product_id"] = out["Final URL"].astype(str).apply(_url_md5)
    return out


def get_unified_filter_options(df: pd.DataFrame) -> dict[str, list[str]]:
    """提取统一分析面板的可选项（网站/关键词/产品）。"""
    if df is None or df.empty:
        return {"domains": [], "keywords": [], "products": []}

    out = df.copy()
    if "domain" not in out.columns:
        if "Domain" in out.columns:
            out["domain"] = out["Domain"].astype(str)
        elif "Final URL" in out.columns:
            out["domain"] = out["Final URL"].astype(str).apply(lambda u: urlparse(u).netloc)
        else:
            out["domain"] = ""

    keyword_col = "Keyword" if "Keyword" in out.columns else ("keyword" if "keyword" in out.columns else None)
    product_col = "Product" if "Product" in out.columns else ("product_title" if "product_title" in out.columns else None)

    domains = sorted([x for x in out["domain"].astype(str).dropna().unique().tolist() if str(x).strip()])
    keywords = sorted([x for x in out[keyword_col].astype(str).dropna().unique().tolist() if str(x).strip()]) if keyword_col else []
    products = sorted([x for x in out[product_col].astype(str).dropna().unique().tolist() if str(x).strip()]) if product_col else []

    return {"domains": domains, "keywords": keywords, "products": products}


def build_unified_analysis_data(
    df: pd.DataFrame,
    domain: str = "",
    keyword: str = "",
    product: str = "",
) -> dict[str, Any]:
    """按网站/关键词/产品过滤，并生成按天变化趋势。"""
    if df is None or df.empty:
        return {
            "filtered": pd.DataFrame(),
            "trend": pd.DataFrame(columns=["date", "records", "unique_products", "avg_price", "avg_reviews"]),
            "summary": {"records": 0, "unique_products": 0, "avg_price": None, "avg_reviews": None},
        }

    out = df.copy()
    if "domain" not in out.columns:
        if "Domain" in out.columns:
            out["domain"] = out["Domain"].astype(str)
        elif "Final URL" in out.columns:
            out["domain"] = out["Final URL"].astype(str).apply(lambda u: urlparse(u).netloc)
        else:
            out["domain"] = ""

    keyword_col = "Keyword" if "Keyword" in out.columns else ("keyword" if "keyword" in out.columns else None)
    product_col = "Product" if "Product" in out.columns else ("product_title" if "product_title" in out.columns else None)

    if domain:
        out = out[out["domain"].astype(str) == str(domain)]
    if keyword and keyword_col:
        out = out[out[keyword_col].astype(str) == str(keyword)]
    if product and product_col:
        out = out[out[product_col].astype(str) == str(product)]

    ts_col = "Timestamp" if "Timestamp" in out.columns else ("observed_at" if "observed_at" in out.columns else None)
    if ts_col:
        ts_source = out[ts_col]
        if isinstance(ts_source, pd.DataFrame):
            ts_source = ts_source.iloc[:, 0]
        ts = pd.to_datetime(ts_source, errors="coerce")
    else:
        ts = pd.Series([pd.NaT] * len(out), index=out.index)

    if isinstance(ts, pd.DatetimeIndex):
        out["__date"] = [v.date() if pd.notna(v) else pd.NaT for v in ts]
    elif isinstance(ts, pd.Series):
        out["__date"] = ts.apply(lambda v: v.date() if pd.notna(v) else pd.NaT)
    else:
        out["__date"] = pd.Series([pd.NaT] * len(out), index=out.index)

    if "Price" in out.columns:
        out["__price"] = pd.to_numeric(out["Price"], errors="coerce")
    else:
        out["__price"] = pd.NA
    if "Review Count" in out.columns:
        out["__reviews"] = pd.to_numeric(out["Review Count"], errors="coerce")
    else:
        out["__reviews"] = pd.NA

    product_count_col = product_col if product_col else "domain"

    trend = (
        out.dropna(subset=["__date"])
        .groupby("__date")
        .agg(
            records=("__date", "size"),
            unique_products=(product_count_col, lambda s: s.astype(str).nunique()),
            avg_price=("__price", "mean"),
            avg_reviews=("__reviews", "mean"),
        )
        .reset_index()
        .rename(columns={"__date": "date"})
        .sort_values("date")
    )

    summary = {
        "records": int(len(out)),
        "unique_products": int(out[product_count_col].astype(str).nunique()) if len(out) > 0 else 0,
        "avg_price": float(pd.to_numeric(out["__price"], errors="coerce").mean()) if len(out) > 0 else None,
        "avg_reviews": float(pd.to_numeric(out["__reviews"], errors="coerce").mean()) if len(out) > 0 else None,
    }

    return {"filtered": out.drop(columns=["__date", "__price", "__reviews"], errors="ignore"), "trend": trend, "summary": summary}


def compute_adgroup_changes(df: pd.DataFrame) -> pd.DataFrame:
    """按天统计广告组变化（new/removed/net_change）。"""
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "adgroup_count", "new_adgroups", "removed_adgroups", "net_change"])

    out = df.copy()
    if "广告组ID" in out.columns:
        grp_col = "广告组ID"
    elif "gad_campaignid" in out.columns:
        grp_col = "gad_campaignid"
    else:
        return pd.DataFrame(columns=["date", "adgroup_count", "new_adgroups", "removed_adgroups", "net_change"])

    ts_col = "Timestamp" if "Timestamp" in out.columns else ("observed_at" if "observed_at" in out.columns else None)
    if not ts_col:
        return pd.DataFrame(columns=["date", "adgroup_count", "new_adgroups", "removed_adgroups", "net_change"])

    dt = pd.to_datetime(out[ts_col], errors="coerce")
    out = out[dt.notna()].copy()
    if out.empty:
        return pd.DataFrame(columns=["date", "adgroup_count", "new_adgroups", "removed_adgroups", "net_change"])
    out["__date"] = dt[dt.notna()].dt.date
    out[grp_col] = out[grp_col].astype(str).fillna("").str.strip()
    out = out[out[grp_col] != ""]
    if out.empty:
        return pd.DataFrame(columns=["date", "adgroup_count", "new_adgroups", "removed_adgroups", "net_change"])

    rows: list[dict[str, Any]] = []
    prev_set: set[str] = set()
    for d in sorted(out["__date"].unique()):
        cur_set = set(out.loc[out["__date"] == d, grp_col].astype(str).tolist())
        added = cur_set - prev_set
        removed = prev_set - cur_set
        rows.append(
            {
                "date": d,
                "adgroup_count": len(cur_set),
                "new_adgroups": len(added),
                "removed_adgroups": len(removed),
                "net_change": len(added) - len(removed),
            }
        )
        prev_set = cur_set
    return pd.DataFrame(rows)


# --- 页面配置 ---
st.set_page_config(page_title="竞品分析 v15 重构版", layout="wide", page_icon="🧠")
st.title("🚀 竞品分析 (v15 架构重构版)")
st.markdown("多轮会话 · 数据对比 · 多模型切换 · 可折叠工作台")

# --- Session State 核心数据结构 ---
if "current_df" not in st.session_state:
    st.session_state.current_df = None
if "current_history_key" not in st.session_state:
    st.session_state.current_history_key = None  # 当前历史文件名或 "_current"

if "selected_domain" not in st.session_state:
    st.session_state.selected_domain = None
if "engine_should_analyze" not in st.session_state:
    st.session_state.engine_should_analyze = False
if "ai_report_content" not in st.session_state:
    st.session_state.ai_report_content = ""
# 当前会话（多轮消息 + 快照），与 current_history_key 对应
if "current_session" not in st.session_state:
    st.session_state.current_session = {
        "id": None,
        "title": "",
        "messages": [],
        "created_at": None,
        "updated_at": None,
        "data_snapshots": [],  # 内存中的 [{timestamp, row_count, description, df}]
    }
# 用于 Diff 的上一快照（仅保留上一个 DataFrame 引用以便对比）
if "prev_snapshot_df" not in st.session_state:
    st.session_state.prev_snapshot_df = None
# LLM 配置（由侧边栏写入，主区读取）
if "llm_platform" not in st.session_state:
    st.session_state.llm_platform = "OpenAI"
if "llm_model" not in st.session_state:
    st.session_state.llm_model = "gpt-5.3-codex"
if "llm_api_key" not in st.session_state:
    st.session_state.llm_api_key = ""
if "llm_base_url" not in st.session_state:
    st.session_state.llm_base_url = "https://api.openai.com/v1"
if "sb_platform" not in st.session_state:
    st.session_state.sb_platform = "OpenAI"
if "sb_model" not in st.session_state:
    st.session_state.sb_model = "gpt-5.3-codex"
if "default_model_bootstrapped" not in st.session_state:
    st.session_state.default_model_bootstrapped = True
    st.session_state.sb_platform = "OpenAI"
    st.session_state.sb_model = "gpt-5.3-codex"
    st.session_state.llm_platform = "OpenAI"
    st.session_state.llm_model = "gpt-5.3-codex"
    st.session_state.llm_base_url = "https://api.openai.com/v1"
# 任务状态机：爬虫运行中 / 暂停
if "is_running" not in st.session_state:
    st.session_state.is_running = True
# 异步报告生成中
if "report_generating" not in st.session_state:
    st.session_state.report_generating = False
# 上一轮抓取数据（用于广告组对比）
if "prev_run_df" not in st.session_state:
    st.session_state.prev_run_df = None

if "history_auto_loaded" not in st.session_state:
    st.session_state.history_auto_loaded = False

if (
    (not st.session_state.history_auto_loaded)
    and st.session_state.current_df is None
    and not st.session_state.get("current_history_key")
):
    try:
        files = list_history_files_with_sessions()
        if files:
            selected_file = files[0]
            loaded_df = load_history_file(selected_file)
            if loaded_df is not None:
                if "广告组ID" not in loaded_df.columns and "Domain" in loaded_df.columns:
                    loaded_df["广告组ID"] = loaded_df["Domain"]
                loaded_df = enrich_dataframe_for_ui(loaded_df, selected_file)
                st.session_state.current_df = loaded_df
                st.session_state.current_history_key = selected_file
                prev_file = _select_prev_history_filename(selected_file, files)
                prev_loaded_df = load_history_file(prev_file) if prev_file else None
                st.session_state.prev_run_df = enrich_dataframe_for_ui(prev_loaded_df, prev_file) if prev_loaded_df is not None else None
                st.session_state._sidebar_selected_file = selected_file  # 同步侧边栏显示
                st.session_state.ai_report_content = ""
                session = get_session_for_file(selected_file)
                st.session_state.current_session = {
                    "id": session.get("id"),
                    "title": session.get("title", selected_file),
                    "messages": session.get("messages", []),
                    "created_at": session.get("created_at"),
                    "updated_at": session.get("updated_at"),
                    "data_snapshots": [],
                }
    except Exception:
        pass
    st.session_state.history_auto_loaded = True


def append_row_to_csv(path: str, row_dict: dict, write_header: bool = False) -> None:
    """追加单行到 CSV，首次可写表头。"""
    file_exists = os.path.exists(path)
    with open(path, "a", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row_dict.keys()))
        if not file_exists or write_header:
            w.writeheader()
        w.writerow(row_dict)


# --- 侧边栏（全部可折叠）---
with st.sidebar:
    st.header("🎛️ 全局控制台")

    # --- 全局域名过滤（置顶，强制影响全页面）---
    try:
        domains = []
        if st.session_state.current_df is not None and not st.session_state.current_df.empty:
            tmp_df = ensure_domain_and_product_id(st.session_state.current_df)
            domains = sorted([d for d in tmp_df["domain"].dropna().astype(str).unique().tolist() if d.strip()])
        if not domains:
            db_domains = load_sqlite_table_as_df(
                "SELECT DISTINCT domain FROM ad_impressions WHERE domain IS NOT NULL AND domain != ''"
            )
            if db_domains is not None and not db_domains.empty:
                domains = sorted([d for d in db_domains["domain"].astype(str).unique().tolist() if d.strip()])
        if domains:
            options = ["混合"] + domains
            if len(domains) == 1 and not st.session_state.get("selected_domain"):
                selected_label = domains[0]
            else:
                selected_label = "混合" if not st.session_state.get("selected_domain") else str(st.session_state.selected_domain)
            default_idx = options.index(selected_label) if selected_label in options else 0
            picked = st.selectbox("选择竞争对手", options=options, index=default_idx)
            st.session_state.selected_domain = None if picked == "混合" else picked
        else:
            st.session_state.selected_domain = None
    except Exception:
        pass

    # --- 侧边栏导航 ---
    page = "📊 总览"

    with st.expander("🗄️ 历史档案管理", expanded=False):
        files = list_history_files_with_sessions()
        # 计算默认选中索引：如果是自动加载的文件，显示它；否则默认第一项
        options = ["-- 请选择 --"] + files
        default_index = 0
        if st.session_state.get("_sidebar_selected_file") and st.session_state._sidebar_selected_file in files:
            default_index = options.index(st.session_state._sidebar_selected_file)
        selected_file = st.selectbox("选择历史记录", options, index=default_index, key="sb_history_select")
        new_file_name = st.text_input("新建文件名", placeholder="例如: Tent_2026_02")
        col_h1, col_h2, col_h3 = st.columns(3)
        if col_h1.button("🆕 新建文件"):
            created_name = create_empty_history_file(new_file_name)
            if created_name:
                st.session_state.current_df = pd.DataFrame()
                st.session_state.current_history_key = created_name
                st.session_state._sidebar_selected_file = created_name
                st.session_state.ai_report_content = ""
                new_session = get_session_for_file(created_name)
                st.session_state.current_session = {
                    "id": new_session.get("id"),
                    "title": new_session.get("title", created_name),
                    "messages": new_session.get("messages", []),
                    "created_at": new_session.get("created_at"),
                    "updated_at": new_session.get("updated_at"),
                    "data_snapshots": [],
                }
                save_session_for_file(created_name, {
                    "id": st.session_state.current_session.get("id"),
                    "title": st.session_state.current_session.get("title"),
                    "messages": st.session_state.current_session.get("messages", []),
                    "created_at": st.session_state.current_session.get("created_at"),
                    "updated_at": st.session_state.current_session.get("updated_at"),
                    "data_snapshots_meta": [],
                })
                st.success(f"已新建: {created_name}")
                st.rerun()
            else:
                st.warning("新建失败：文件名为空、非法或已存在")

        if col_h1.button("📂 加载数据"):
            if selected_file and selected_file != "-- 请选择 --":
                loaded_df = load_history_file(selected_file)
                if loaded_df is not None:
                    if "广告组ID" not in loaded_df.columns and "Domain" in loaded_df.columns:
                        loaded_df["广告组ID"] = loaded_df["Domain"]
                    loaded_df = enrich_dataframe_for_ui(loaded_df, selected_file)
                    st.session_state.current_df = loaded_df
                    st.session_state.current_history_key = selected_file
                    prev_file = _select_prev_history_filename(selected_file, files)
                    prev_loaded_df = load_history_file(prev_file) if prev_file else None
                    st.session_state.prev_run_df = enrich_dataframe_for_ui(prev_loaded_df, prev_file) if prev_loaded_df is not None else None
                    st.session_state._sidebar_selected_file = selected_file  # 同步侧边栏显示
                    st.session_state.ai_report_content = ""
                    # 加载该历史记录对应的多轮会话
                    session = get_session_for_file(selected_file)
                    st.session_state.current_session = {
                        "id": session.get("id"),
                        "title": session.get("title", selected_file),
                        "messages": session.get("messages", []),
                        "created_at": session.get("created_at"),
                        "updated_at": session.get("updated_at"),
                        "data_snapshots": [],
                    }
                    st.success(f"已加载: {selected_file}，可继续多轮对话")
                    st.rerun()
                else:
                    st.error(f"加载失败：{selected_file} 不存在或内容不可读取")
        rename_to = st.text_input("重命名为", placeholder="例如: Tent_Analysis")
        if col_h2.button("✏️ 重命名"):
            if selected_file == "-- 请选择 --":
                st.warning("请先选择要重命名的文件")
            elif not rename_to:
                st.warning("请输入新文件名")
            else:
                if rename_history_file(selected_file, rename_to):
                    final_name = rename_to if rename_to.endswith(".csv") else f"{rename_to}.csv"
                    if st.session_state.get("current_history_key") == selected_file:
                        st.session_state.current_history_key = final_name
                    st.session_state._sidebar_selected_file = final_name
                    st.success("重命名成功！")
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error("重命名失败：源文件不存在或目标文件已存在")

        if col_h3.button("🧾 刷新列表"):
            st.rerun()

    with st.expander("📡 新建抓取任务", expanded=True):
        blocked_domains_input = st.text_input(
            "🚫 屏蔽域名 (逗号分隔，留空不屏蔽)",
            value="",
            placeholder="例如 yourbrand.com, shop.yourbrand.com",
        )
        st.caption("已移除专项计划输入；历史文件名仅按时间与关键词生成。")
        keywords_input = st.text_area("🔑 关键词列表 (逗号分隔)", value="ultralight tent, camping chair", height=80)
        c_set1, c_set2 = st.columns(2)
        max_workers = c_set1.slider("并发窗口", 1, 4, 1)  # 降低默认并发，减少异常
        pages_to_scrape = c_set2.slider("单词页数", 1, 3, 1)
        enable_global_lock = st.toggle("🌍 全球锁定 (模拟当地搜索)", value=True)
        target_region = st.selectbox(
            "目标市场",
            ["🇺🇸 美国 (us/en)", "🇬🇧 英国 (uk/en)", "🇩🇪 德国 (de/de)"],
            disabled=not enable_global_lock,
        )
        force_headless = st.checkbox("🙈 后台静默运行", value=False)
        use_proxy = st.checkbox("启用代理 IP", value=False)
        proxy_url = st.text_input("代理地址", placeholder="http://127.0.0.1:7890", disabled=not use_proxy)
        auto_refresh = st.checkbox("自动刷新进度(低频)", value=False)
        refresh_interval = st.select_slider("刷新间隔(秒)", options=[2, 3, 5, 8, 10], value=5, disabled=not auto_refresh)
        start_btn = st.button("🚀 启动强力引擎", type="primary", width="stretch")
        if st.button("⏹️ 停止并分析", key="stop_analyze_btn"):
            with ENGINE_LOCK:
                ENGINE_STATE["stop_requested"] = True
            st.session_state.engine_should_analyze = True
            st.session_state.is_running = False
            st.toast("已发送停止信号：将关闭窗口并分析已加载数据...")
            st.rerun()

        if st.button("🧹 重置引擎(卡死用)", key="reset_engine_btn", width="stretch"):
            with ENGINE_LOCK:
                ENGINE_STATE["stop_requested"] = True
                ENGINE_STATE["running"] = False
                ENGINE_STATE["error"] = None
                ENGINE_STATE["pending"] = 0
                ENGINE_STATE["discovered"] = 0
                ENGINE_STATE["done"] = 0
                ENGINE_STATE["fail"] = 0
                ENGINE_STATE["shared_list"] = None
                ENGINE_STATE["transferred"] = False
            st.session_state.is_running = False
            st.session_state.engine_should_analyze = False
            st.toast("已重置引擎状态，可重新启动。若仍有残留窗口，请手动关闭。")
            st.rerun()

        st.divider()

    with st.expander("🤖 AI 指挥中心 (点击展开/收起)", expanded=True):
        platform_options = list(PLATFORM_MODELS.keys())
        if st.session_state.get("sb_platform") not in platform_options:
            st.session_state.sb_platform = "OpenAI"

        platform = st.selectbox("Select Platform", platform_options, key="sb_platform")
        models = PLATFORM_MODELS.get(platform, [])

        if st.session_state.get("sb_model") not in models:
            if platform == "OpenAI" and "gpt-5.3-codex" in models:
                st.session_state.sb_model = "gpt-5.3-codex"
            elif models:
                st.session_state.sb_model = models[0]

        model_name = st.selectbox("Select Model", models, key="sb_model")
        api_key = st.text_input("API Key", type="password", value=os.environ.get("OPENAI_API_KEY", ""), key="sb_apikey")
        # Base URL 使用常量，不展示在 UI
        base_url = DEFAULT_BASE_URLS.get(platform, "")
        st.session_state.llm_platform = platform
        st.session_state.llm_model = model_name
        st.session_state.llm_api_key = api_key

    with st.expander("🛠️ 引擎状态调试 (点击展开)", expanded=False):
        with ENGINE_LOCK:
            dbg = {
                k: ENGINE_STATE.get(k)
                for k in [
                    "running",
                    "stop_requested",
                    "discovered",
                    "pending",
                    "done",
                    "fail",
                    "error",
                    "save_filename",
                    "transferred",
                ]
            }
        st.json(dbg)
        if st.button("🔎 检测 8501 端口占用", key="dbg_check_8501"):
            pids = _get_listening_pids_on_port(8501)
            st.write({"port": 8501, "listening_pids": pids})
            if len(pids) >= 2:
                st.warning("检测到多个进程监听 8501：这会导致页面连到旧进程，按钮/进度异常。请结束多余 PID 后重启。")
        st.session_state.llm_base_url = base_url
        if st.button("🧪 测试 API 连通性"):
            if not api_key and platform != "Ollama":
                st.error("请先输入 API Key")
            else:
                try:
                    provider = get_provider(platform, model_name, api_key, base_url)
                    if provider and provider.test_connection():
                        st.success("✅ API 连接畅通！")
                    else:
                        st.info("Ollama 本地无需 Key，请确保服务已启动")
                except Exception as e:
                    st.error(f"❌ 连接失败: {e}")
        st.caption("📝 深度报告生成规则 (Prompt)")
        default_prompt = "请作为一位资深跨境电商运营专家，分析以下竞品数据。\n请总结：\n1. 市场上的热门产品卖点是什么？\n2. 竞品主要采用了什么广告策略（图文/搜索）？\n3. 头部竞品的分布情况如何？\n4. 如果我要跟卖，有什么差异化建议？"
        global_ai_rule = st.text_area("在此修改生成规则", value=default_prompt, height=120, key="sb_prompt")
        if st.button("⚡ 立即生成/重绘报告"):
            if st.session_state.current_df is None:
                st.error("请先抓取或加载数据！")
            elif not api_key and platform != "Ollama":
                st.error("请填写 API Key！")
            else:
                if "report_error" in st.session_state:
                    del st.session_state.report_error
                try:
                    st.session_state.report_generating = True
                    df = st.session_state.current_df
                    provider = get_provider(st.session_state.llm_platform, st.session_state.llm_model, st.session_state.llm_api_key, st.session_state.llm_base_url)
                    if provider:
                        summary = df["Product"].value_counts().head(10).to_string()
                        cols = [c for c in ["Keyword", "Product", "Domain", "Type"] if c in df.columns]
                        data_sample = df[cols].head(40).to_csv(index=False) if cols else ""
                        full_prompt = chr(10).join([global_ai_rule, "", "[Summary]", summary, "", "[Data Sample Top40]", data_sample])
                        content_placeholder = st.empty()
                        full_response = ""
                        stream_result = provider.chat([{"role": "user", "content": full_prompt}], stream=True)
                        for chunk in stream_result:
                            full_response += chunk
                            content_placeholder.markdown(full_response + "▌")
                        content_placeholder.markdown(full_response)

                        st.session_state.ai_report_content = full_response
                        snap = make_snapshot(df, "生成报告时")
                        st.session_state.current_session.setdefault("data_snapshots", []).append(snap)
                except Exception as e:
                    st.session_state.report_error = str(e)
                finally:
                    st.session_state.report_generating = False

# --- 抓取逻辑（保留 V1）---
def get_product_name(url):
    try:
        path = urlparse(url).path
        name = path.split("/")[-1].replace("-", " ").replace(".html", "").title()
        if not name:
            return "Home Page / Brand"
        return name[:50]
    except Exception:
        return "Unknown"


def resolve_url_fast(url, proxy_url=None):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"}
    proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
    try:
        # 对 Google Ads 点击链接（/aclk）优先从参数中直接提取落地页
        try:
            from urllib.parse import parse_qs, urlparse

            u = urlparse(url)
            if "/aclk" in (u.path or ""):
                qs = parse_qs(u.query)
                for k in ["adurl", "url", "q"]:
                    v = qs.get(k)
                    if v and v[0].startswith("http"):
                        return v[0]
        except Exception:
            pass

        resp = requests.get(url, headers=headers, proxies=proxies, allow_redirects=True, timeout=10)
        # 不再用 "google.com" 做否定判断：/aclk 经常会停留在 google 跳转层
        return resp.url
    except Exception:
        return None


# --- 生产者：在主线程中扫描页面并推入队列（DrissionPage 必须主线程调用）---
def producer_fill_queue_main_thread(browser, k_list, settings, link_queue, total_discovered, ui_placeholders):
    """在主线程执行，便于每步刷新看板。ui_placeholders = (m_total, m_done, m_pending, m_fail, p_bar)。"""
    base = DEFAULT_TARGET_URL.rstrip("/")
    m_total, m_done, m_pending, m_fail, p_bar = ui_placeholders
    for ki, keyword in enumerate(k_list):
        if not st.session_state.is_running:
            break
        tab = None
        try:
            tab = browser.new_tab()
            gl, hl = settings["gl"], settings["hl"]
            for i in range(settings["pages"]):
                search_url = f"{base}/search?q={keyword}&start={i*10}&gl={gl}&hl={hl}&pws=0"
                try:
                    tab.get(search_url, timeout=25)
                    time.sleep(2)
                    tab.scroll.to_bottom()
                    time.sleep(1)
                    tab.scroll.to_top()
                    time.sleep(0.5)
                except Exception as e:
                    print(f"Page load {keyword} p{i}: {e}")
                try:
                    links = tab.eles("xpath://a[@href]")
                    for link in links:
                        try:
                            href = link.attr("href")
                            if href and "/aclk" in href:
                                link_queue.put((keyword, href))
                                total_discovered[0] += 1
                        except Exception:
                            continue
                except Exception as e:
                    print(f"Extract links {keyword}: {e}")
                # 每页后刷新看板
                m_total.metric("🔍 发现链接总数", total_discovered[0])
                m_pending.metric("⏳ 待处理", link_queue.qsize())
                time.sleep(random.uniform(1, 2))
            if tab:
                try:
                    tab.close()
                except Exception:
                    pass
                tab = None
        except Exception as e:
            print(f"Producer keyword {keyword}: {e}")
        finally:
            if tab:
                try:
                    tab.close()
                except Exception:
                    pass
    # 结束信号：每个 worker 一个
    for _ in range(settings.get("num_workers", 2)):
        link_queue.put(None)


def consumer_worker(link_queue, shared_list, lock, counters, is_running, settings, save_path):
    """从队列取 (keyword, raw_url)，解析后符合条件则追加到 shared_list 并写文件。"""
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }

    def _brand_fallback_from_domain(domain: str) -> str:
        base = domain.split(":")[0].lower()
        parts = base.split(".")
        if len(parts) >= 2:
            core = parts[-2]
        else:
            core = base
        return core[:1].upper() + core[1:]

    def _extract_brand(html: str, domain: str) -> str:
        try:
            m = re.search(r'<meta[^>]+property=["\']og:site_name["\'][^>]+content=["\']([^"\']+)["\']', html, re.I)
            if m and m.group(1).strip():
                return m.group(1).strip()[:80]
            m = re.search(r'<title[^>]*>(.*?)</title>', html, re.I | re.S)
            if m:
                title = re.sub(r"\s+", " ", m.group(1)).strip()
                if title:
                    # 常见格式：Brand - Product / Product | Brand
                    for sep in ["|", "-", "–", "—"]:
                        if sep in title:
                            cand = title.split(sep)[-1].strip()
                            if 2 <= len(cand) <= 40:
                                return cand
        except Exception:
            pass
        return _brand_fallback_from_domain(domain)

    def _page_type(url: str) -> str:
        u = (url or "").lower()
        if any(x in u for x in ["/product/", "/products/", "/item/", "/p/"]):
            return "Product Page (详情页)"
        if any(x in u for x in ["/collection/", "/collections/", "/category/"]):
            return "Collection (列表页)"
        return "Other/Home"

    def _detect_mainstream_ad_signals(url: str, html: str = "") -> tuple[bool, list[str]]:
        """检测是否为主流平台广告，返回 (is_ad, signals)"""
        signals = []
        try:
            from urllib.parse import parse_qs, urlparse
            qs = parse_qs(urlparse(url).query)
            url_lower = url.lower()
            # Google Ads 点击签名：/aclk
            if "/aclk" in url_lower:
                signals.append("/aclk")
            # Google 广告信号
            if "gclid" in qs:
                signals.append("gclid")
            if any(k.lower() == "gad_source" and v == ["1"] for k, v in qs.items()):
                signals.append("gad_source=1")
            if any(k.lower() == "gad_campaignid" for k, v in qs.items()):
                signals.append("gad_campaignid")
            if any(k.lower() == "gbraid" for k, v in qs.items()):
                signals.append("gbraid")
            if html and 'data-ved' in html:
                signals.append("data-ved")
            # YouTube 广告信号
            if "si" in qs:
                signals.append("si")
            if any(k.lower() in {"feature", "lc", "pp"} for k, v in qs.items()):
                signals.append("youtube_params")
            if "ab_channel" in qs:
                signals.append("ab_channel")
            # Meta/Facebook/Instagram 广告信号
            if "fbclid" in qs:
                signals.append("fbclid")
            if "igshid" in qs:
                signals.append("igshid")
            if any(v and v[0].lower() in {"facebook", "instagram", "fb"} for k, v in qs.items() if k.lower() == "utm_source"):
                signals.append("utm_source=meta")
            # TikTok 广告信号
            if "ttclid" in qs:
                signals.append("ttclid")
            if any(v and v[0].lower() == "tiktok" for k, v in qs.items() if k.lower() == "utm_source"):
                signals.append("utm_source=tiktok")
            # X/Twitter 广告信号
            if "twclid" in qs:
                signals.append("twclid")
            if any(v and v[0].lower() in {"twitter", "x"} for k, v in qs.items() if k.lower() == "utm_source"):
                signals.append("utm_source=twitter")
            # LinkedIn 广告信号
            if "li_fat_id" in qs:
                signals.append("li_fat_id")
            if any(v and v[0].lower() == "linkedin" for k, v in qs.items() if k.lower() == "utm_source"):
                signals.append("utm_source=linkedin")
            # Bing 广告信号
            if "msclkid" in qs:
                signals.append("msclkid")
            # 通用广告信号
            if any(v and v[0].lower() in {"cpc", "ppc"} for k, v in qs.items() if k.lower() == "utm_medium"):
                signals.append("utm_medium=cpc/ppc")
            # 通用组合：utm_campaign + utm_source（常见广告标记）
            if "utm_campaign" in qs and any(k.lower() == "utm_source" for k, v in qs.items()):
                signals.append("utm_campaign+source")
        except Exception:
            pass
        return (len(signals) > 0, signals)

    while is_running[0]:
        try:
            item = link_queue.get(timeout=1)
            if item is None:
                break
            keyword, raw_url = item
            final_url = resolve_url_fast(raw_url, settings["proxy"])
            if not final_url:
                with lock:
                    counters["fail"] += 1
                continue
            blocked_domains = settings.get("blocked_domains", [])
            final_domain = urlparse(final_url).netloc
            if _is_blocked_domain(final_domain, blocked_domains):
                continue
            ad_type = "Shopping (图)" if "shopping" in raw_url else "Search (文)"
            domain = final_domain

            # --- 稳定性加固：requests 单例抓取 + 解析不全标记待分析（模块二）---
            parse_strategy = "A"
            title = get_product_name(final_url)
            price = None
            review_count = None
            http_status = None
            error_msg = None
            html_snapshot_id = None
            brand = _brand_fallback_from_domain(domain)
            page_type = _page_type(final_url)

            proxies = {"http": settings.get("proxy"), "https": settings.get("proxy")} if settings.get("proxy") else None
            html = ""
            shopify_price = None
            shopify_currency = None
            shopify_compare_at = None
            shopify_is_available = None
            shopify_review_count = None
            image_url = None
            try:
                resp = session.get(final_url, headers=headers, proxies=proxies, timeout=10)
                http_status = resp.status_code
                html = resp.text or ""
                brand = _extract_brand(html, domain)
                try:
                    m_img = re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', html, re.I)
                    if m_img and m_img.group(1).strip():
                        image_url = m_img.group(1).strip()[:500]
                except Exception:
                    pass
                # 标题兜底
                m_title = re.search(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']', html, re.I)
                if m_title and m_title.group(1).strip():
                    title = m_title.group(1).strip()[:120]
                else:
                    m_title2 = re.search(r'<title[^>]*>(.*?)</title>', html, re.I | re.S)
                    if m_title2:
                        title = re.sub(r"\s+", " ", m_title2.group(1)).strip()[:120] or title
                # 价格/评论启发式
                price = infer_price_from_text(html[:4000])
                if price is None:
                    m_meta_price = re.search(r'<meta[^>]+property=["\']product:price:amount["\'][^>]+content=["\']([^"\']+)["\']', html, re.I)
                    if m_meta_price:
                        try:
                            price = float(str(m_meta_price.group(1)).replace(",", "").strip())
                        except Exception:
                            pass
                review_count = _extract_review_count_from_html(html[:15000])

                # Shopify 商品详情解析（JSON-LD + OG availability）
                try:
                    shopify = parse_shopify_product(html)
                    shopify_price = shopify.get("price")
                    shopify_currency = shopify.get("currency")
                    shopify_compare_at = shopify.get("compare_at_price")
                    shopify_is_available = shopify.get("is_available")
                    shopify_review_count = shopify.get("review_count")
                    if shopify_price is not None:
                        price = shopify_price
                    if review_count is None and shopify_review_count is not None:
                        review_count = shopify_review_count
                except Exception as e:
                    # 解析失败不影响主流程
                    error_msg = (error_msg + " | " if error_msg else "") + f"shopify_parse:{e}"
            except Exception as e:
                parse_strategy = "B_PENDING"
                error_msg = str(e)

            # --- 广告标记：能访问但不是广告 => 标记 Is Ad=False，不算失败 ---
            # 注意：raw_url 可能包含 /aclk，因此把 raw_url 与 final_url 都纳入信号判断
            is_ad_1, ad_signals_1 = _detect_mainstream_ad_signals(raw_url, "")
            is_ad_2, ad_signals_2 = _detect_mainstream_ad_signals(final_url, html)
            ad_signals = list(dict.fromkeys(ad_signals_1 + ad_signals_2))
            is_ad = bool(ad_signals)

            # Search(文) 或解析缺失：保留 HTML 快照并标记待分析，不算失败
            need_snapshot = (ad_type == "Search (文)") or (not title) or (price is None)
            if need_snapshot:
                if parse_strategy == "A":
                    parse_strategy = "B_PENDING"
                try:
                    html_snapshot_id = f"{sku_fingerprint(final_url)}_{int(datetime.now().timestamp())}"
                    snapshot_path = os.path.join(HISTORY_DIR, "snapshots", f"{html_snapshot_id}.html")
                    os.makedirs(os.path.dirname(snapshot_path), exist_ok=True)
                    with open(snapshot_path, "w", encoding="utf-8") as f:
                        f.write(html if html else ("<!-- fetch failed -->\n" + final_url))
                except Exception:
                    html_snapshot_id = None

            row = {
                "Campaign": settings.get("campaign", ""),
                "Campaign ID": _extract_campaign_id_from_url(final_url),
                "Batch ID": settings.get("batch_id", ""),
                "Keyword": keyword,
                "Product": title,
                "Brand": brand,
                "Domain": domain,
                "广告组ID": _extract_adgroup_id_from_url(final_url) or _extract_campaign_id_from_url(final_url) or domain,
                "Page Type": page_type,
                "Type": ad_type,
                "Is Ad": is_ad,
                "Ad Signals": ", ".join(ad_signals),
                "Final URL": final_url,
                "sku_id": sku_fingerprint(final_url),
                "Price": price,
                "Review Count": review_count,
                "Currency": shopify_currency,
                "Is Available": shopify_is_available,
                "Compare At Price": shopify_compare_at,
                "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "parse_strategy": parse_strategy,
                "http_status": http_status,
                "html_snapshot_id": html_snapshot_id,
                "error_msg": error_msg,
            }

            # --- 入库（不阻断主流程）---
            try:
                observed_at = row.get("Timestamp")
                product_id = db_upsert_product_and_state(
                    url=final_url,
                    domain=domain,
                    title=title,
                    image_url=image_url,
                    observed_at=observed_at,
                    price=price if price is not None else None,
                    compare_at_price=shopify_compare_at,
                    is_available=shopify_is_available,
                    currency=shopify_currency,
                )
                db_insert_ad_impression(
                    observed_at=observed_at,
                    keyword=keyword,
                    gad_campaignid=_extract_campaign_id_from_url(final_url),
                    url=final_url,
                    raw_url=raw_url,
                    domain=domain,
                    brand=brand,
                    ad_type=ad_type,
                    ad_signals=", ".join(ad_signals),
                    product_id=product_id,
                    batch_id=settings.get("batch_id", ""),
                    campaign=settings.get("campaign", ""),
                )
            except Exception as e:
                row["error_msg"] = (row.get("error_msg") + " | " if row.get("error_msg") else "") + f"db:{e}"
            with lock:
                shared_list.append(row)
                counters["done"] += 1
                append_row_to_csv(save_path, row, write_header=(len(shared_list) == 1))
        except queue.Empty:
            continue
        except Exception as e:
            with lock:
                counters["fail"] += 1
            print(f"Consumer error: {e}")


# --- 执行抓取（后台线程 + 可中断）---
if start_btn:
    k_list = [k.strip() for k in keywords_input.split(",") if k.strip()]
    if not k_list:
        st.error("请输入至少一个关键词")
    else:
        with ENGINE_LOCK:
            already_running = bool(ENGINE_STATE.get("running"))
        if already_running:
            st.warning("引擎正在运行中，请先停止或等待完成。")
        else:
            st.session_state.is_running = True
            st.session_state.engine_should_analyze = False
            # 保存“上一轮”用于广告组对比
            st.session_state.prev_run_df = st.session_state.current_df.copy() if st.session_state.current_df is not None else None

            proxy_setting = proxy_url if use_proxy else None
            gl_code, hl_code = "us", "en"
            if enable_global_lock:
                if "英国" in target_region:
                    gl_code, hl_code = "uk", "en"
                elif "德国" in target_region:
                    gl_code, hl_code = "de", "de"

            task_start = datetime.now().strftime("%Y%m%d_%H%M%S")
            current_save_filename = None
            current_save_path = None
            existing_key = st.session_state.get("current_history_key")
            if isinstance(existing_key, str) and existing_key.endswith(".csv"):
                p = os.path.join(HISTORY_DIR, existing_key)
                if os.path.exists(p):
                    current_save_filename = existing_key
                    current_save_path = p
                    try:
                        with open(current_save_path, "w", encoding="utf-8-sig") as _f:
                            _f.write("")
                    except Exception:
                        pass
            if not current_save_filename or not current_save_path:
                safe_summary = "".join([c for c in k_list[0][:20] if c.isalnum() or c in (" ", "_")]).strip()
                current_save_filename = f"{task_start}_{safe_summary}.csv"
                current_save_path = os.path.join(HISTORY_DIR, current_save_filename)

            settings = {
                "proxy": proxy_setting,
                "blocked_domains": _parse_blocked_domains(blocked_domains_input),
                "campaign": "",
                "pages": pages_to_scrape,
                "gl": gl_code,
                "hl": hl_code,
                "num_workers": max_workers,
                "batch_id": task_start,
                "force_headless": force_headless,
            }

            th = threading.Thread(
                target=_engine_thread_run,
                args=(k_list, settings, current_save_path, current_save_filename),
                daemon=True,
            )
            th.start()
            st.toast("引擎已在后台启动，可随时点击「停止并分析」。")
            st.rerun()

# 引擎运行中：显示实时看板并自动刷新
with ENGINE_LOCK:
    eng_running = bool(ENGINE_STATE.get("running"))
    eng_err = ENGINE_STATE.get("error")
    eng_done = int(ENGINE_STATE.get("done") or 0)
    eng_fail = int(ENGINE_STATE.get("fail") or 0)
    eng_pending = int(ENGINE_STATE.get("pending") or 0)
    eng_disc = int(ENGINE_STATE.get("discovered") or 0)
    eng_save = ENGINE_STATE.get("save_filename")
    eng_shared = ENGINE_STATE.get("shared_list")
    eng_transferred = bool(ENGINE_STATE.get("transferred"))

if eng_running:
    st.divider()
    st.subheader("📊 实时进度监控看板（后台运行）")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("🔍 发现链接总数", eng_disc)
    col2.metric("✅ 已完成", eng_done)
    col3.metric("⏳ 待处理", eng_pending)
    col4.metric("❌ 失败/异常", eng_fail)
    if st.button("🔄 刷新进度", key="engine_refresh_btn"):
        st.rerun()
    if auto_refresh:
        time.sleep(float(refresh_interval))
        st.rerun()

# 引擎结束：如果用户点了停止并分析（或自然结束），把数据切换到分析区（仅一次）
if (
    (not eng_running)
    and (not eng_transferred)
    and eng_shared is not None
    and len(eng_shared) > 0
    and (st.session_state.get("engine_should_analyze") or st.session_state.get("is_running"))
):
    st.session_state.current_df = pd.DataFrame(list(eng_shared))
    st.session_state.current_history_key = eng_save
    st.session_state.current_session = {"id": None, "title": "", "messages": [], "created_at": None, "updated_at": None, "data_snapshots": []}
    st.session_state.is_running = False
    st.session_state.engine_should_analyze = False
    with ENGINE_LOCK:
        ENGINE_STATE["transferred"] = True
    st.toast(f"数据已自动持久化: {eng_save}")
    st.rerun()

if (not eng_running) and eng_err:
    st.error(f"引擎启动失败: {eng_err}")


# --- 主内容：数据分析工作台（总览）---
if page == "📊 总览" and st.session_state.current_df is not None:
    df = enrich_dataframe_for_ui(st.session_state.current_df.copy(), st.session_state.current_history_key or "")
    df = ensure_domain_and_product_id(df)
    if st.session_state.get("selected_domain") and "domain" in df.columns:
        df = df[df["domain"] == st.session_state.selected_domain]
    if "广告组ID" not in df.columns and "Domain" in df.columns:
        df["广告组ID"] = df["Domain"]

    st.divider()
    st.header(f"📈 数据分析工作台 (共 {len(df)} 条)")
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button("📥 下载当前数据 CSV", csv_bytes, "analysis_data.csv", "text/csv", key="dl_csv_main")

    tab_report, tab_ai, tab_adgroup, tab_focus, tab_decoder, tab_diff = st.tabs([
        "📄 深度报告",
        "💬 AI 数据顾问",
        "📁 广告组视图",
        "🎯 单维度分析",
        "🧩 投放策略解密 (Ad Strategy Decoder)",
        "⚖️ Batch Diff",
    ])

    with tab_report:
        with st.expander("📑 深度报告 (点击展开)", expanded=True):
            if getattr(st.session_state, "report_generating", False):
                st.info("🚀 深度报告生成中... 请稍后刷新或再次切换到此 Tab 查看。")
            elif getattr(st.session_state, "report_error", None):
                st.error(f"报告生成失败: {st.session_state.report_error}")
            elif st.session_state.ai_report_content:
                st.info("💡 如需修改报告方向，请在侧边栏「AI 指挥中心」修改生成规则并重新生成。")
                st.markdown(st.session_state.ai_report_content)
            else:
                st.warning("👈 暂无报告。请在左侧「AI 指挥中心」配置 Platform/Model 与 API Key 后点击「立即生成/重绘报告」。")

    with tab_ai:
        chat_key = st.session_state.current_history_key or "_current"
        current_session = st.session_state.current_session
        messages = current_session.get("messages", [])

        with st.expander("💬 多轮数据查询与对比 (点击展开)", expanded=True):
            st.caption("在当前历史记录内多轮提问，可要求对比前几轮结论。")
            for msg in messages:
                with st.chat_message(msg.get("role", "user")):
                    st.markdown(msg.get("content", ""))

            if prompt := st.chat_input("例如：根据报告主推什么款式？或：对比前几轮分析结果"):
                api_key = st.session_state.get("llm_api_key", "")
                platform = st.session_state.get("sb_platform", "OpenAI")
                if not api_key and platform != "Ollama":
                    st.error("请在侧边栏「AI 指挥中心」配置 API Key")
                else:
                    messages.append({"role": "user", "content": prompt})
                    with st.chat_message("user"):
                        st.markdown(prompt)

                    data_cols = [c for c in ["Product", "Domain", "Type", "广告组ID"] if c in df.columns]
                    data_context = df[data_cols].head(50).to_csv(index=False) if data_cols else ""
                    report_context = st.session_state.ai_report_content or "暂未生成正式报告"
                    system_msg = f"""你是精通电商数据的分析助手。用户会在同一历史记录内多轮提问，可能要求对比之前几轮的分析结论。

【当前数据片段】:
{data_context}

【已生成的战术分析报告】:
{report_context}

请结合以上信息回答；若用户要求「对比」或「和之前相比」，请综合本对话中之前的问答进行对比分析。"""

                    try:
                        provider = get_provider(st.session_state.llm_platform, st.session_state.llm_model, st.session_state.llm_api_key, st.session_state.llm_base_url)
                        with st.chat_message("assistant"):
                            message_placeholder = st.empty()
                            full_response = ""
                            msgs = [{"role": "system", "content": system_msg}] + [{"role": m["role"], "content": m["content"]} for m in messages[-10:]]
                            stream_result = provider.chat(msgs, stream=True)
                            for chunk in stream_result:
                                full_response += chunk
                                message_placeholder.markdown(full_response + "▌")
                            message_placeholder.markdown(full_response)
                        messages.append({"role": "assistant", "content": full_response})
                        current_session["messages"] = messages
                        if chat_key != "_current":
                            save_session_for_file(chat_key, {
                                "id": current_session.get("id"),
                                "title": current_session.get("title"),
                                "messages": current_session["messages"],
                                "created_at": current_session.get("created_at"),
                                "updated_at": current_session.get("updated_at"),
                                "data_snapshots_meta": [{"timestamp": s.get("timestamp"), "row_count": s.get("row_count"), "description": s.get("description", "")} for s in current_session.get("data_snapshots", [])],
                            })
                    except Exception as e:
                        st.error(f"对话服务出错: {e}")

    with tab_adgroup:
        df_parsed, df_dedup = adparser_enrich_and_dedup(df)

        t_raw, t_tree, t_change = st.tabs(["视图 1：原始数据表", "视图 2：广告活动架构（树状图）", "视图 3：广告组变化"])

        with t_raw:
            show_cols = [
                c
                for c in [
                    "Timestamp",
                    "Brand",
                    "gad_campaignid",
                    "Product",
                    "出现次数",
                    "Type",
                    "Final URL",
                    "Keyword",
                    "parse_strategy",
                    "http_status",
                    "error_msg",
                ]
                if c in df_parsed.columns
            ]
            raw_df_show = df_parsed[show_cols].copy() if show_cols else df_parsed
            col_cfg = {}
            if "Final URL" in raw_df_show.columns:
                col_cfg["Final URL"] = st.column_config.LinkColumn("URL", display_text="打开")
            st.dataframe(raw_df_show, width="stretch", column_config=col_cfg)

        with t_tree:
            with st.expander("📁 Brand → Campaign ID(gad_campaignid) → Products（去重后）", expanded=True):
                if df_dedup is None or df_dedup.empty:
                    st.info("暂无数据")
                else:
                    # 只展示具备 gad_campaignid 的广告树
                    tree_df = df_dedup.copy()
                    if "gad_campaignid" not in tree_df.columns:
                        tree_df["gad_campaignid"] = ""
                    tree_df["gad_campaignid"] = tree_df["gad_campaignid"].fillna("")
                    tree_df = tree_df[tree_df["gad_campaignid"].astype(str).str.strip().ne("")]
                    if tree_df.empty:
                        st.info("没有解析到 gad_campaignid（无法构建树状图）。")
                    else:
                        if "Brand" not in tree_df.columns:
                            tree_df["Brand"] = ""
                        for brand, brand_df in tree_df.groupby("Brand", sort=False):
                            brand_name = brand if str(brand).strip() else "(Unknown Brand)"
                            with st.expander(f"🏷️ 品牌: {brand_name}（{len(brand_df)} 条广告）", expanded=False):
                                for cid, cid_df in brand_df.groupby("gad_campaignid", sort=False):
                                    cid_name = cid if str(cid).strip() else "(No gad_campaignid)"
                                    with st.expander(f"📌 Campaign ID: {cid_name}（{len(cid_df)} 个产品）", expanded=False):
                                        show_cols2 = [
                                            c
                                            for c in [
                                                "product_name",
                                                "出现次数",
                                                "adgroupid",
                                                "Price",
                                                "Review Count",
                                                "Timestamp",
                                                "Final URL",
                                            ]
                                            if c in cid_df.columns
                                        ]
                                        view_df = cid_df[show_cols2].copy() if show_cols2 else cid_df
                                        col_cfg2 = {}
                                        if "Final URL" in view_df.columns:
                                            col_cfg2["Final URL"] = st.column_config.LinkColumn("URL", display_text="打开")
                                        st.dataframe(view_df, width="stretch", column_config=col_cfg2)

        with t_change:
            st.markdown("**广告组变化（按日期）**")
            domain_opts = sorted([d for d in df_parsed.get("Domain", pd.Series(dtype="object")).astype(str).dropna().unique().tolist() if str(d).strip()]) if isinstance(df_parsed, pd.DataFrame) and not df_parsed.empty else []
            adg_domain_pick = st.selectbox("选择网站（广告组变化）", ["全部"] + domain_opts, index=0, key="adg_domain_pick")
            adg_base = df_parsed.copy() if isinstance(df_parsed, pd.DataFrame) else pd.DataFrame()
            if adg_domain_pick != "全部" and not adg_base.empty and "Domain" in adg_base.columns:
                adg_base = adg_base[adg_base["Domain"].astype(str) == adg_domain_pick]
            adg_change_df = compute_adgroup_changes(adg_base)
            if adg_change_df is None or adg_change_df.empty:
                st.info("当前筛选下暂无可用广告组时间变化数据（需要 Timestamp + 广告组ID/gad_campaignid）。")
            else:
                adg_chart = adg_change_df.copy()
                adg_chart["date"] = pd.to_datetime(adg_chart["date"], errors="coerce")
                adg_chart = adg_chart.dropna(subset=["date"]).set_index("date")
                cols = [c for c in ["adgroup_count", "new_adgroups", "removed_adgroups", "net_change"] if c in adg_chart.columns]
                if cols and not adg_chart.empty:
                    st.line_chart(adg_chart[cols], height=220)
                st.dataframe(adg_change_df, width="stretch")

    with tab_focus:
        st.subheader("🎯 单网站 / 单关键词 / 单产品联合分析")
        opts = get_unified_filter_options(df)

        col_f1, col_f2, col_f3 = st.columns(3)
        domain_pick = col_f1.selectbox("网站", ["全部"] + opts["domains"], index=0, key="focus_domain")
        keyword_pick = col_f2.selectbox("关键词", ["全部"] + opts["keywords"], index=0, key="focus_keyword")
        product_pick = col_f3.selectbox("产品", ["全部"] + opts["products"], index=0, key="focus_product")

        result = build_unified_analysis_data(
            df,
            domain="" if domain_pick == "全部" else domain_pick,
            keyword="" if keyword_pick == "全部" else keyword_pick,
            product="" if product_pick == "全部" else product_pick,
        )

        focus_df = result["filtered"]
        trend_df = result["trend"]
        summary = result["summary"]

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("记录数", int(summary.get("records", 0) or 0), None)
        m2.metric("产品数", int(summary.get("unique_products", 0) or 0), None)
        avg_price = summary.get("avg_price")
        avg_reviews = summary.get("avg_reviews")
        m3.metric("均价", f"{avg_price:.2f}" if avg_price is not None and pd.notna(avg_price) else "-")
        m4.metric("平均评论", f"{avg_reviews:.2f}" if avg_reviews is not None and pd.notna(avg_reviews) else "-")

        total_n = int(len(focus_df)) if focus_df is not None else 0
        if total_n > 0:
            price_cov = float(pd.to_numeric(focus_df["Price"], errors="coerce").notna().mean() * 100.0) if "Price" in focus_df.columns else 0.0
            review_cov = float(pd.to_numeric(focus_df["Review Count"], errors="coerce").notna().mean() * 100.0) if "Review Count" in focus_df.columns else 0.0
            st.caption(f"价格识别覆盖率: {price_cov:.1f}% | 评论识别覆盖率: {review_cov:.1f}%")
            if "Page Type" in focus_df.columns:
                page_dist = focus_df["Page Type"].astype(str).value_counts(dropna=False)
                collection_ratio = float(page_dist.get("Collection (列表页)", 0) / max(total_n, 1) * 100.0)
                if collection_ratio >= 60:
                    st.info(f"当前筛选中列表页占比 {collection_ratio:.1f}%，列表页通常不稳定提供单品价格/评论，建议切到产品页样本观察。")
            if price_cov < 20 or review_cov < 20:
                st.warning("当前站点价格/评论抓取覆盖率较低，常见原因是页面 JS 动态渲染或反爬限制。建议增加采样轮次或切换代理后重试。")

        if trend_df is not None and not trend_df.empty:
            st.markdown("**时间变化趋势（日维度）**")
            chart_cols = [c for c in ["records", "unique_products", "avg_price", "avg_reviews"] if c in trend_df.columns]
            chart_df = trend_df.copy()
            chart_df["date"] = pd.to_datetime(chart_df["date"], errors="coerce")
            chart_df = chart_df.dropna(subset=["date"]).set_index("date")
            if chart_cols and not chart_df.empty:
                st.line_chart(chart_df[chart_cols], height=260)

                first_row = chart_df.iloc[0]
                last_row = chart_df.iloc[-1]
                d1, d2, d3 = st.columns(3)
                d1.metric("记录变化", int(last_row.get("records", 0)), f"{int(last_row.get('records', 0) - first_row.get('records', 0)):+d}")
                if "avg_price" in chart_df.columns and pd.notna(first_row.get("avg_price")) and pd.notna(last_row.get("avg_price")):
                    d2.metric("均价变化", f"{float(last_row.get('avg_price')):.2f}", f"{(float(last_row.get('avg_price')) - float(first_row.get('avg_price'))):+.2f}")
                else:
                    d2.metric("均价变化", "-", None)
                if "avg_reviews" in chart_df.columns and pd.notna(first_row.get("avg_reviews")) and pd.notna(last_row.get("avg_reviews")):
                    d3.metric("评论均值变化", f"{float(last_row.get('avg_reviews')):.2f}", f"{(float(last_row.get('avg_reviews')) - float(first_row.get('avg_reviews'))):+.2f}")
                else:
                    d3.metric("评论均值变化", "-", None)
            st.dataframe(trend_df, width="stretch")
        else:
            st.info("当前筛选条件下暂无可用时间序列数据（需要 Timestamp 或 observed_at 字段）。")

        st.markdown("**筛选结果明细（前 200 条）**")
        show_cols = [
            c
            for c in [
                "Timestamp",
                "observed_at",
                "Domain",
                "domain",
                "Keyword",
                "keyword",
                "Product",
                "product_title",
                "Price",
                "Review Count",
                "Final URL",
            ]
            if c in focus_df.columns
        ]
        focus_show = focus_df[show_cols].head(200) if show_cols else focus_df.head(200)
        col_cfg_focus = {}
        if "Final URL" in focus_show.columns:
            col_cfg_focus["Final URL"] = st.column_config.LinkColumn("URL", display_text="打开")
        st.dataframe(focus_show, width="stretch", column_config=col_cfg_focus)

    with tab_decoder:
        st.subheader("🧩 投放策略解密 (Ad Strategy Decoder)")
        try:
            selected_domain = st.session_state.get("selected_domain")
            ad_df = load_sqlite_table_as_df(
                """
                SELECT observed_at, keyword, gad_campaignid, url, raw_url, domain, brand, ad_type, ad_signals, product_id, batch_id
                FROM ad_impressions
                WHERE url IS NOT NULL AND url != ''
                """
            )
            prod_df = load_sqlite_table_as_df(
                """
                SELECT id AS product_id, domain AS product_domain, title AS product_title, handle AS product_handle, image_url AS product_image_url, first_seen_at
                FROM products
                """
            )
            if ad_df is None or ad_df.empty:
                st.info("SQLite 中暂无 ad_impressions 数据。请先抓取一轮广告。")
            else:
                scoped_ad_df = ad_df.copy()
                scope_note = "当前视图"
                # 优先按当前数据里的 Batch ID 限定，避免混入历史库旧数据
                if "Batch ID" in df.columns:
                    batch_ids = [b for b in df["Batch ID"].astype(str).dropna().unique().tolist() if str(b).strip()]
                    if batch_ids and "batch_id" in scoped_ad_df.columns:
                        scoped_ad_df = scoped_ad_df[scoped_ad_df["batch_id"].astype(str).isin(batch_ids)]

                # 若没有 Batch ID 或过滤后为空，按当前数据日期限定
                if scoped_ad_df.empty and "Timestamp" in df.columns:
                    dates = sorted(
                        pd.to_datetime(df["Timestamp"], errors="coerce").dropna().dt.strftime("%Y-%m-%d").unique().tolist()
                    )
                    if dates:
                        mask = pd.Series([False] * len(ad_df), index=ad_df.index)
                        obs = ad_df["observed_at"].astype(str)
                        for d in dates:
                            mask = mask | obs.str.startswith(d)
                        scoped_ad_df = ad_df[mask]

                # 仍为空则回退全库（并提示）
                if scoped_ad_df.empty:
                    scoped_ad_df = ad_df.copy()
                    scope_note = "全库回退"

                st.caption(f"解密分析数据范围：{scope_note}，记录数 {len(scoped_ad_df)}")

                x = scoped_ad_df.merge(prod_df, on="product_id", how="left")
                if selected_domain:
                    x = x[x["domain"] == selected_domain]
                x["observed_at"] = pd.to_datetime(x["observed_at"], errors="coerce")
                x["landing_type"] = x["url"].astype(str).apply(classify_landing_page)

                t1, t2, t3, t4 = st.tabs([
                    "1) 账户结构逆向",
                    "2) 落地页承接策略",
                    "3) Hero Product Matrix",
                    "4) Keyword-Product Match",
                ])

                with t1:
                    st.caption("按 gad_campaignid 聚合：sku_count=不同落地页数，keyword_count=投放词数，并按规则标注策略。")
                    g = (
                        x.groupby("gad_campaignid", dropna=False)
                        .agg(
                            sku_count=("url", "nunique"),
                            keyword_count=("keyword", "nunique"),
                            impressions=("url", "size"),
                        )
                        .reset_index()
                    )

                    def _label_strategy(row):
                        try:
                            sku_count = int(row.get("sku_count") or 0)
                            keyword_count = int(row.get("keyword_count") or 0)
                            if sku_count == 1 and keyword_count < 5:
                                return "单品打爆策略 (SPAG)"
                            if sku_count > 20:
                                return "通投测品策略 (General)"
                            return "Mixed/Other"
                        except Exception:
                            return "Mixed/Other"

                    g["strategy"] = g.apply(_label_strategy, axis=1)
                    st.dataframe(g.sort_values(["sku_count", "keyword_count"], ascending=False), width="stretch")

                    chart = (
                        alt.Chart(g[g["gad_campaignid"].astype(str).str.len() > 0])
                        .mark_bar()
                        .encode(
                            x=alt.X("gad_campaignid:N", sort="-y", title="Campaign ID (gad_campaignid)"),
                            y=alt.Y("sku_count:Q", title="SKU Count (unique final_url)"),
                            color=alt.Color("strategy:N", title="Strategy"),
                            tooltip=["gad_campaignid", "sku_count", "keyword_count", "impressions", "strategy"],
                        )
                        .properties(height=320)
                    )
                    st.altair_chart(chart, width="stretch")

                with t2:
                    st.caption("按落地页路径特征分类：PDP / Collection / Home，并统计分布。")
                    lp = x.groupby("landing_type").size().reset_index(name="impressions")
                    st.dataframe(lp.sort_values("impressions", ascending=False), width="stretch")
                    pie = (
                        alt.Chart(lp)
                        .mark_arc()
                        .encode(
                            theta=alt.Theta("impressions:Q"),
                            color=alt.Color("landing_type:N", title="Landing Type"),
                            tooltip=["landing_type", "impressions"],
                        )
                        .properties(height=320)
                    )
                    st.altair_chart(pie, width="stretch")

                with t3:
                    st.caption("按 product_handle 统计广告频次与关键词覆盖，识别主推款（Hero Product）。")
                    tmp = x.copy()
                    tmp["product_handle"] = tmp["product_handle"].fillna("")
                    tmp["product_title"] = tmp["product_title"].fillna("")
                    hero = (
                        tmp[tmp["product_handle"].astype(str).str.len() > 0]
                        .groupby("product_handle")
                        .agg(
                            title=("product_title", "first"),
                            image_url=("product_image_url", "first"),
                            ad_frequency=("url", "size"),
                            keyword_count=("keyword", "nunique"),
                            campaign_count=("gad_campaignid", "nunique"),
                            sample_url=("url", "first"),
                        )
                        .reset_index()
                        .sort_values(["ad_frequency", "keyword_count"], ascending=False)
                        .head(10)
                    )

                    # 关键词列表（Top10 行才拼，避免爆内存）
                    kw_map = (
                        tmp[tmp["product_handle"].isin(hero["product_handle"])][["product_handle", "keyword"]]
                        .dropna()
                        .groupby("product_handle")["keyword"]
                        .apply(lambda s: ", ".join(sorted(set([str(v).strip() for v in s if str(v).strip()]))[:50]))
                        .to_dict()
                    )
                    hero["keywords"] = hero["product_handle"].map(kw_map).fillna("")

                    # 可读展示：隐藏 sku_id，仅展示 title/handle，并把 handle 做成可点击链接
                    hero_show = hero.copy()
                    hero_show["handle_link"] = hero_show["sample_url"]
                    cols = [
                        c
                        for c in [
                            "image_url",
                            "title",
                            "handle_link",
                            "ad_frequency",
                            "keyword_count",
                            "campaign_count",
                            "keywords",
                        ]
                        if c in hero_show.columns
                    ]
                    hero_show = hero_show[cols]
                    col_cfg = {
                        "image_url": st.column_config.ImageColumn("Img", width="small"),
                        "handle_link": st.column_config.LinkColumn("Handle", display_text="打开"),
                    }
                    st.dataframe(hero_show, width="stretch", column_config=col_cfg)

                with t4:
                    st.caption("判断关键词是否出现在标题中，用于识别‘乱投词’机会点。")
                    tmp = x.copy()
                    tmp["keyword_norm"] = tmp["keyword"].astype(str).fillna("").str.lower().str.strip()
                    tmp["title_norm"] = tmp["product_title"].astype(str).fillna("").str.lower()

                    def _match(row):
                        kw = row.get("keyword_norm", "")
                        title = row.get("title_norm", "")
                        if kw and title and kw in title:
                            return "精准匹配"
                        return "弱相关"

                    tmp["match_type"] = tmp.apply(_match, axis=1)
                    m = tmp.groupby("match_type").size().reset_index(name="impressions")
                    c1, c2 = st.columns(2)
                    c1.metric("精准匹配", int(m[m["match_type"] == "精准匹配"]["impressions"].sum() if not m.empty else 0))
                    c2.metric("弱相关", int(m[m["match_type"] == "弱相关"]["impressions"].sum() if not m.empty else 0))
                    st.dataframe(m, width="stretch")

                    weak = (
                        tmp[tmp["match_type"] == "弱相关"][
                            ["observed_at", "keyword", "product_title", "gad_campaignid", "url"]
                        ]
                        .sort_values("observed_at", ascending=False)
                        .head(50)
                    )
                    col_cfg = {}
                    if "url" in weak.columns:
                        col_cfg["url"] = st.column_config.LinkColumn("URL", display_text="打开")
                    st.dataframe(weak, width="stretch", column_config=col_cfg)
        except Exception as e:
            st.error(f"Ad Strategy Decoder 加载/分析失败：{e}")

    with tab_diff:
        from data_diff import batch_diff
        snapshots = st.session_state.current_session.get("data_snapshots", [])
        prev_df = st.session_state.get("prev_run_df")
        with st.expander("⚖️ 数据全景对比 (Batch Diff)", expanded=True):
            # 时序对比引擎（模块三）
            if prev_df is not None and not df.empty:
                st.subheader("📊 与上一轮抓取对比（基于 sku_id）")
                prev_df2 = ensure_domain_and_product_id(enrich_dataframe_for_ui(prev_df.copy(), ""))
                curr_df2 = df.copy()
                # 全局域名过滤
                if st.session_state.get("selected_domain") and "domain" in prev_df2.columns and "domain" in curr_df2.columns:
                    prev_df2 = prev_df2[prev_df2["domain"] == st.session_state.selected_domain]
                    curr_df2 = curr_df2[curr_df2["domain"] == st.session_state.selected_domain]

                diff_result = batch_diff(prev_df2, curr_df2)
                summary = diff_result["summary"]
                c1, c2, c3, c4 = st.columns(4)
                prev_count = int(summary.get("prev_count", 0) or 0)
                curr_count = int(summary.get("curr_count", 0) or 0)
                delta = summary.get("delta", None)
                try:
                    delta_text = f"{int(delta):+d}" if delta is not None else None
                except Exception:
                    delta_text = None
                c1.metric("上一轮 SKU", prev_count, None)
                c2.metric("本轮 SKU", curr_count, delta_text)
                c3.metric("🆕 新增", int(summary.get("new_count", 0) or 0), None)
                c4.metric("🗑️ 移除", int(summary.get("removed_count", 0) or 0), None)

                # 可读 Diff 表
                items = diff_result.get("items", [])
                if items:
                    diff_df = pd.DataFrame(items)
                    # 使用 canonical_url 计算 product_id，并 join products 变为可读数据
                    try:
                        diff_df["product_id"] = diff_df["canonical_url"].astype(str).apply(_url_md5)
                    except Exception:
                        diff_df["product_id"] = ""

                    prod_map = sql_df(
                        """
                        SELECT id AS product_id, title AS product_title, handle AS product_handle, image_url AS product_image_url
                        FROM products
                        WHERE domain = ?
                        """,
                        params=(st.session_state.get("selected_domain") or "",),
                    )
                    if prod_map is not None and not prod_map.empty:
                        diff_df = diff_df.merge(prod_map, on="product_id", how="left")
                    if "product_title" not in diff_df.columns:
                        diff_df["product_title"] = ""
                    diff_df["product_title"] = diff_df["product_title"].fillna("").astype(str)
                    diff_df["title_short"] = diff_df["product_title"].apply(lambda s: (s[:50] + "…") if len(s) > 50 else s)
                    diff_df["full_title"] = diff_df["product_title"]
                    diff_df["open_link"] = diff_df.get("canonical_url", "")

                    # join 标题/价格（来自 prev/curr）
                    prev_map = prev_df2.set_index("sku_id") if "sku_id" in prev_df2.columns else None
                    curr_map = curr_df2.set_index("sku_id") if "sku_id" in curr_df2.columns else None

                    def _safe_get(m, key, col):
                        try:
                            if m is not None and key in m.index and col in m.columns:
                                v = m.loc[key, col]
                                if isinstance(v, pd.Series):
                                    v = v.dropna().iloc[0] if not v.dropna().empty else None
                                return v
                        except Exception:
                            pass
                        return None

                    def _has_text(v) -> bool:
                        if v is None:
                            return False
                        try:
                            if pd.isna(v):
                                return False
                        except Exception:
                            pass
                        return str(v).strip() != ""

                    rows = []
                    for it in items:
                        sku = it.get("sku_id")
                        status = it.get("status")
                        title_new = _safe_get(curr_map, sku, "Product")
                        title_old = _safe_get(prev_map, sku, "Product")
                        price_new = _safe_get(curr_map, sku, "Price")
                        price_old = _safe_get(prev_map, sku, "Price")

                        title = title_new if _has_text(title_new) else title_old
                        if status == "new":
                            rows.append({"Status": "New", "Title": title, "New Price": price_new})
                        elif status == "modified":
                            try:
                                pct = None
                                if price_old not in [None, ""] and float(price_old) != 0 and price_new not in [None, ""]:
                                    pct = (float(price_new) - float(price_old)) / float(price_old) * 100
                            except Exception:
                                pct = None
                            rows.append({"Status": "Modified", "Title": title, "Old Price": price_old, "New Price": price_new, "Change %": pct})
                    if rows:
                        show = pd.DataFrame(rows)
                        st.dataframe(show, width="stretch")

                    # 主可读 Diff 表：图片/标题/链接/状态/价格变化（隐藏 sku_id）
                    st.subheader("📋 Diff 明细（可读版）")
                    show2 = diff_df.copy()
                    # 将价格变化列拉平
                    show2["Status"] = show2["status"].map({"new": "New", "removed": "Removed", "modified": "Modified", "unchanged": "Unchanged"}).fillna(show2["status"])
                    show2["Price Δ"] = show2.get("price_change")
                    show2["Change %"] = show2.get("price_change_pct")
                    display_cols = [
                        c
                        for c in [
                            "product_image_url",
                            "title_short",
                            "full_title",
                            "open_link",
                            "Status",
                            "Price Δ",
                            "Change %",
                        ]
                        if c in show2.columns
                    ]
                    col_cfg = {}
                    if "open_link" in show2.columns:
                        col_cfg["open_link"] = st.column_config.LinkColumn("打开", display_text="打开")
                    if "product_image_url" in show2.columns:
                        col_cfg["product_image_url"] = st.column_config.ImageColumn("图片", width="small")
                    st.dataframe(show2[display_cols], width="stretch", column_config=col_cfg)
                c4.metric("🔄 变更", summary["modified"], None)
                st.divider()
                # 价格变化汇总
                c5, c6, c7 = st.columns(3)
                c5.metric("💰 降价", summary["price_down"], None)
                c6.metric("📈 涨价", summary["price_up"], None)
                c7.metric("⭐ 评论增长", summary["review_growing"], None)
                # 详细 diff 表格（高亮）
                if items:
                    diff_df_display = pd.DataFrame(items)
                    # 添加箭头列
                    def price_arrow(row):
                        if row["price_change"] is None:
                            return "—"
                        if row["price_change"] < 0:
                            return f"🟢↓{abs(row['price_change']):.2f}"
                        elif row["price_change"] > 0:
                            return f"🔴↑{row['price_change']:.2f}"
                        else:
                            return "—"
                    diff_df_display["价格变化"] = diff_df_display.apply(price_arrow, axis=1)
                    # 展示列
                    with st.expander("调试：原始 diff（含 sku_id）", expanded=False):
                        display_cols = ["sku_id", "status", "价格变化", "domain", "canonical_url"]
                        col_cfg3 = {}
                        if "canonical_url" in diff_df_display.columns:
                            col_cfg3["canonical_url"] = st.column_config.LinkColumn("URL", display_text="打开")
                        st.dataframe(diff_df_display[display_cols].head(50), width="stretch", column_config=col_cfg3)
                    # SKU 趋势图入口（模块四）
                    st.caption("💡 点击上方 sku_id 可查看历史趋势（后续扩展）")
                else:
                    st.info("无变化数据。")
                st.divider()
            # 兼容旧版快照 diff（保留）
            if len(snapshots) >= 2:
                st.subheader("📂 会话内快照 Diff（旧版兼容）")
                prev_snap = snapshots[-2]
                curr_snap = snapshots[-1]
                prev_df_snap = prev_snap.get("df") if isinstance(prev_snap, dict) else None
                curr_df_snap = curr_snap.get("df") if isinstance(curr_snap, dict) else None
                if prev_df_snap is not None and curr_df_snap is not None:
                    diff_result_old = compute_diff(prev_df_snap, curr_df_snap)
                    summary_old = get_diff_summary_for_ui(diff_result_old)
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("上一快照 行数", summary_old["row_count_prev"], None)
                    c2.metric("当前快照 行数", summary_old["row_count_curr"], f"{summary_old['row_delta']:+d}")
                    c3.metric("📈 新增", summary_old["added_count"], None)
                    c4.metric("📉 移除", summary_old["removed_count"], None)
                    for col, vals in summary_old.get("numeric_deltas", {}).items():
                        st.metric(f"数值列「{col}」均值", vals["curr"], f"{vals['delta']:+.2f}")
                    with st.expander("查看详细变更 (新增/移除的 ID 或行)"):
                        st.write("**新增:** ", summary_old["added_ids"][:50])
                        st.write("**移除:** ", summary_old["removed_ids"][:50])
                else:
                    st.caption("仅保留快照元数据，无完整 DataFrame，无法计算详细 Diff。下次生成报告后将保留快照用于对比。")
            else:
                st.info("👈 暂无历史数据可供对比。请先抓取一次数据，再抓取第二次即可看到 Batch Diff。")

        with st.expander("🔎 单品深度分析", expanded=False):
            try:
                selected_domain = st.session_state.get("selected_domain")
                if selected_domain:
                    drill = load_sqlite_table_as_df(
                        """
                        SELECT ai.product_id, p.title AS product_title, p.handle AS product_handle, p.image_url AS product_image_url,
                               COUNT(*) AS ad_frequency
                        FROM ad_impressions ai
                        LEFT JOIN products p ON p.id = ai.product_id
                        WHERE ai.domain = ?
                        GROUP BY ai.product_id
                        ORDER BY ad_frequency DESC
                        LIMIT 20
                        """.replace("?", f"'{selected_domain}'")
                    )
                    if drill is None or drill.empty:
                        st.caption("当前域名下暂无可用于深度分析的商品（请先抓取并入库）。")
                    else:
                        drill["label"] = drill.apply(
                            lambda r: f"{r.get('product_title','') or r.get('product_handle','')} ({r.get('ad_frequency',0)})",
                            axis=1,
                        )
                        sel = st.selectbox("选择商品（Top 20 by Ad Frequency）", options=drill["label"].tolist(), index=0)
                        product_id = drill.loc[drill["label"] == sel, "product_id"].iloc[0]

                        states = load_sqlite_table_as_df(
                            f"SELECT observed_at, price FROM product_states WHERE product_id = '{product_id}' ORDER BY observed_at"
                        )
                        imps = load_sqlite_table_as_df(
                            f"SELECT observed_at FROM ad_impressions WHERE product_id = '{product_id}' ORDER BY observed_at"
                        )

                        if states is not None and not states.empty:
                            states["observed_at"] = pd.to_datetime(states["observed_at"], errors="coerce")
                            chart1 = (
                                alt.Chart(states.dropna(subset=["observed_at"]))
                                .mark_line(point=True)
                                .encode(
                                    x=alt.X("observed_at:T", title="时间"),
                                    y=alt.Y("price:Q", title="Price"),
                                    tooltip=[
                                        alt.Tooltip("observed_at:T", title="Time"),
                                        alt.Tooltip("price:Q", title="Price"),
                                    ],
                                )
                                .properties(height=260, title="价格走势")
                            )
                            st.altair_chart(chart1, width="stretch")
                        else:
                            st.caption("暂无价格状态记录")

                        if imps is not None and not imps.empty:
                            imps["observed_at"] = pd.to_datetime(imps["observed_at"], errors="coerce")
                            freq = imps.groupby(pd.Grouper(key="observed_at", freq="D")).size().reset_index(name="impressions")
                            chart2 = (
                                alt.Chart(freq.dropna(subset=["observed_at"]))
                                .mark_line(point=True)
                                .encode(
                                    x=alt.X("observed_at:T", title="时间"),
                                    y=alt.Y("impressions:Q", title="Ad Impressions (Daily)"),
                                    tooltip=[
                                        alt.Tooltip("observed_at:T", title="Date"),
                                        alt.Tooltip("impressions:Q", title="Impressions"),
                                    ],
                                )
                                .properties(height=260, title="广告投放力度（按天）")
                            )
                            st.altair_chart(chart2, width="stretch")
                        else:
                            st.caption("暂无广告曝光记录")
                else:
                    st.caption("请先在左侧选择竞争对手域名。")
            except Exception as e:
                st.error(f"单品深度分析加载失败：{e}")

else:
    st.divider()
    st.header("📊 总览")
    st.info("当前还没有可分析的数据。你可以在左侧先『📂 加载数据』或启动抓取引擎生成数据。")
    tab_report, tab_ai, tab_adgroup, tab_decoder, tab_diff = st.tabs([
        "📄 深度报告",
        "💬 AI 数据顾问",
        "📁 广告组视图",
        "🧩 投放策略解密 (Ad Strategy Decoder)",
        "⚖️ Batch Diff",
    ])
    with tab_report:
        st.caption("请先加载历史数据或完成一次抓取后再生成报告。")
    with tab_ai:
        st.caption("请先加载历史数据或完成一次抓取后再进行数据问答。")
    with tab_adgroup:
        st.caption("请先加载历史数据或完成一次抓取后再查看广告组视图。")
    with tab_decoder:
        st.caption("请先抓取并入库后再进行投放策略解密分析。")
    with tab_diff:
        st.caption("请先完成两轮抓取（或加载包含上一轮数据的历史记录）后再查看 Batch Diff。")
        with st.expander("🔎 单品深度分析", expanded=False):
            st.caption("入口已收敛为仅在『⚖️ Batch Diff』里展示。加载/抓取数据后可用。")
