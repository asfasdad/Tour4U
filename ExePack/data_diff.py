# -*- coding: utf-8 -*-
"""
数据差异对比 (Diff)：快照机制 + 集合运算
用于同一会话内多次查询/筛选后的数据变化展示。
"""
import pandas as pd
from typing import Tuple, List, Optional, Any, Dict
from datetime import datetime
import hashlib
import re
import json
from urllib.parse import urlparse


# --- SKU 指纹与 Batch Diff 结构化输出（模块一/三）---
def normalize_url(url: str) -> str:
    """URL 规范化：去 query/fragment、统一 scheme/host 小写"""
    try:
        p = urlparse(url.strip().lower())
        return f"{p.scheme}://{p.netloc}{p.path.rstrip('/')}"
    except Exception:
        return url.strip().lower()


def sku_fingerprint(url: str) -> str:
    """生成 SKU 指纹（sha256(normalized_url)）"""
    normalized = normalize_url(url)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def infer_price_from_text(text: str) -> Optional[float]:
    """启发式价格提取（兜底方案 B1）"""
    patterns = [
        r"\$\s*([\d,]+\.?\d*)",
        r"([\d,]+\.?\d*)\s*\$",
        r"¥\s*([\d,]+\.?\d*)",
        r"([\d,]+\.?\d*)\s*¥",
        r"€\s*([\d,]+\.?\d*)",
        r"([\d,]+\.?\d*)\s*€",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            try:
                return float(m.group(1).replace(",", ""))
            except Exception:
                continue
    return None


def batch_diff(prev_df: Optional[pd.DataFrame], curr_df: pd.DataFrame) -> Dict[str, Any]:
    """
    时序对比引擎（模块三）：
    - 基于 sku_id 对齐
    - 输出结构化 diff_items（new/removed/modified）
    - 计算价格变化幅度与语义状态
    """
    # 确保 sku_id 列存在
    if "sku_id" not in curr_df.columns:
        curr_df = curr_df.copy()
        curr_df["sku_id"] = curr_df.get("Final URL", curr_df.get("URL", "")).apply(sku_fingerprint)
    if prev_df is not None and "sku_id" not in prev_df.columns:
        prev_df = prev_df.copy()
        prev_df["sku_id"] = prev_df.get("Final URL", prev_df.get("URL", "")).apply(sku_fingerprint)

    prev_map = {row["sku_id"]: row for _, row in (prev_df.iterrows() if prev_df is not None else [])}
    curr_map = {row["sku_id"]: row for _, row in curr_df.iterrows()}

    all_skus = set(prev_map) | set(curr_map)

    diff_items = []
    for sku in all_skus:
        prev = prev_map.get(sku)
        curr = curr_map.get(sku)
        # 初始化变量，避免 new/removed 分支访问未定义
        price_change = None
        price_change_pct = None
        review_delta = None
        changes = {}
        if prev is None and curr is not None:
            status = "new"
        elif prev is not None and curr is None:
            status = "removed"
        else:
            # Modified / Unchanged 判定
            changed_fields = []
            # 价格变化
            try:
                price_prev = float(prev.get("Price", 0) or 0) if prev is not None else 0
                price_curr = float(curr.get("Price", 0) or 0) if curr is not None else 0
                if price_prev != price_curr:
                    changed_fields.append("price")
                    price_change = price_curr - price_prev
                    price_change_pct = (price_change / price_prev * 100) if price_prev != 0 else None
                    changes["price"] = {"from": price_prev, "to": price_curr, "delta": price_change, "pct": price_change_pct}
            except Exception:
                pass
            # 评论数变化
            try:
                rev_prev = int(prev.get("Review Count", 0) or 0) if prev is not None else 0
                rev_curr = int(curr.get("Review Count", 0) or 0) if curr is not None else 0
                if rev_prev != rev_curr:
                    changed_fields.append("review_count")
                    review_delta = rev_curr - rev_prev
                    changes["review_count"] = {"from": rev_prev, "to": rev_curr, "delta": review_delta}
            except Exception:
                pass
            # 可扩展其他字段（rating/availability/等）
            status = "modified" if changed_fields else "unchanged"

        item = {
            "sku_id": sku,
            "status": status,
            "semantic": [],
            "changes": changes if status == "modified" else {},
            "price_change": price_change,
            "price_change_pct": price_change_pct,
            "review_delta": review_delta,
            "domain": curr.get("Domain") if curr is not None else (prev.get("Domain") if prev is not None else ""),
            "canonical_url": curr.get("Final URL") if curr is not None else (prev.get("Final URL") if prev is not None else ""),
        }
        # 语义状态
        if status == "modified":
            if price_change is not None:
                if price_change < 0:
                    item["semantic"].append("discounted")
                elif price_change > 0:
                    item["semantic"].append("price_up")
            if review_delta is not None and review_delta > 0:
                item["semantic"].append("review_growing")
        diff_items.append(item)

    # 汇总统计
    prev_count = len(prev_map)
    curr_count = len(curr_map)
    new_count = sum(1 for it in diff_items if it["status"] == "new")
    removed_count = sum(1 for it in diff_items if it["status"] == "removed")
    modified_count = sum(1 for it in diff_items if it["status"] == "modified")
    unchanged_count = sum(1 for it in diff_items if it["status"] == "unchanged")
    summary = {
        # 兼容旧字段
        "total": len(diff_items),
        "new": new_count,
        "removed": removed_count,
        "modified": modified_count,
        "unchanged": unchanged_count,
        "price_down": sum(1 for it in diff_items if "discounted" in it["semantic"]),
        "price_up": sum(1 for it in diff_items if "price_up" in it["semantic"]),
        "review_growing": sum(1 for it in diff_items if "review_growing" in it["semantic"]),
        # 新增字段：用于 UI 统计卡片
        "prev_count": prev_count,
        "curr_count": curr_count,
        "delta": curr_count - prev_count,
        "new_count": new_count,
        "removed_count": removed_count,
    }
    return {"summary": summary, "items": diff_items}


def _infer_id_column(df: pd.DataFrame) -> Optional[str]:
    """推断用作唯一标识的列：优先 sku_id，否则 SKU，否则 Product+Domain，否则第一列"""
    if df is None or df.empty:
        return None
    if "sku_id" in df.columns:
        return "sku_id"
    if "SKU" in df.columns:
        return "SKU"
    if "Product" in df.columns and "Domain" in df.columns:
        return None  # 用复合键
    return df.columns[0] if len(df.columns) > 0 else None


def _get_row_ids(df: pd.DataFrame, id_col: Optional[str]) -> set:
    """得到行的“唯一标识”集合。若无 id_col 则用 (Product, Domain) 或整行 tuple."""
    if df is None or df.empty:
        return set()
    if id_col and id_col in df.columns:
        return set(df[id_col].astype(str).dropna().unique())
    if "Product" in df.columns and "Domain" in df.columns:
        return set(zip(df["Product"].astype(str), df["Domain"].astype(str)))
    return set(df.iloc[:, 0].astype(str).dropna().unique())


def compute_diff(
    prev_df: Optional[pd.DataFrame],
    curr_df: pd.DataFrame,
    id_column: Optional[str] = None,
) -> dict:
    """
    计算两个 DataFrame 的差异（集合运算）。
    返回:
      - added_ids: 当前有、上一版没有的 ID
      - removed_ids: 上一版有、当前没有的 ID
      - added_count, removed_count
      - row_count_prev, row_count_curr
      - numeric_deltas: 可聚合数值列的变化 { col: {"prev": v1, "curr": v2, "delta": v2-v1 } }
    """
    id_col = id_column or _infer_id_column(curr_df)
    curr_ids = _get_row_ids(curr_df, id_col)
    prev_ids = _get_row_ids(prev_df, id_col) if prev_df is not None else set()

    added_ids = curr_ids - prev_ids
    removed_ids = prev_ids - curr_ids

    row_count_prev = len(prev_df) if prev_df is not None else 0
    row_count_curr = len(curr_df)

    numeric_deltas = {}
    if curr_df is not None and not curr_df.empty:
        numeric_cols = curr_df.select_dtypes(include=["number"]).columns.tolist()
        for col in numeric_cols:
            try:
                curr_val = curr_df[col].mean()
                prev_val = prev_df[col].mean() if prev_df is not None and col in prev_df.columns else None
                if prev_val is None:
                    continue
                delta = curr_val - prev_val
                numeric_deltas[col] = {"prev": round(prev_val, 2), "curr": round(curr_val, 2), "delta": round(delta, 2)}
            except Exception:
                pass

    return {
        "added_ids": added_ids,
        "removed_ids": removed_ids,
        "added_count": len(added_ids),
        "removed_count": len(removed_ids),
        "row_count_prev": row_count_prev,
        "row_count_curr": row_count_curr,
        "numeric_deltas": numeric_deltas,
        "id_column": id_col,
    }


def make_snapshot(df: pd.DataFrame, description: str = "") -> dict:
    """生成当前数据的快照（用于会话内存储）。"""
    return {
        "timestamp": datetime.now().isoformat(),
        "row_count": len(df) if df is not None else 0,
        "description": description,
        "df": df.copy() if df is not None else None,
    }


def get_diff_summary_for_ui(diff_result: dict, id_col_display: str = "行") -> dict:
    """
    整理为 UI 可用的摘要：行数变化、核心数值变化、新增/移除的 ID 列表（用于 expander）。
    """
    row_prev = diff_result.get("row_count_prev", 0)
    row_curr = diff_result.get("row_count_curr", 0)
    added = diff_result.get("added_count", 0)
    removed = diff_result.get("removed_count", 0)
    id_col = diff_result.get("id_column") or id_col_display

    return {
        "row_count_prev": row_prev,
        "row_count_curr": row_curr,
        "row_delta": row_curr - row_prev,
        "added_count": added,
        "removed_count": removed,
        "added_ids": list(diff_result.get("added_ids", []))[:100],
        "removed_ids": list(diff_result.get("removed_ids", []))[:100],
        "numeric_deltas": diff_result.get("numeric_deltas", {}),
        "id_column": id_col,
    }
