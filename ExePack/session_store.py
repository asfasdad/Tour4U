# -*- coding: utf-8 -*-
"""
会话管理：多轮会话链与历史记录绑定
HistoryItem = { id, title, messages, created_at, updated_at }
持久化到 history_data/sessions.json，按 history_file 键存储。
"""
import os
import json
import sys
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional

if getattr(sys, "frozen", False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

HISTORY_DIR = os.path.join(BASE_DIR, "history_data")
SESSIONS_FILE = os.path.join(HISTORY_DIR, "sessions.json")


def _ensure_dir():
    os.makedirs(HISTORY_DIR, exist_ok=True)


def _load_sessions_raw() -> Dict[str, Any]:
    _ensure_dir()
    if not os.path.exists(SESSIONS_FILE):
        return {"by_file": {}, "by_id": {}}
    try:
        with open(SESSIONS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"by_file": {}, "by_id": {}}


def _save_sessions_raw(data: Dict[str, Any]) -> None:
    _ensure_dir()
    with open(SESSIONS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _serialize_message(m: Dict) -> Dict:
    return {"role": m.get("role", "user"), "content": m.get("content", "")}


def _serialize_snapshot(s: Dict) -> Dict:
    """只持久化快照元数据，不存完整 DataFrame（体积大）"""
    return {
        "timestamp": s.get("timestamp"),
        "row_count": s.get("row_count"),
        "description": s.get("description", ""),
    }


def get_session_for_file(history_filename: str) -> Dict[str, Any]:
    """
    获取某条历史记录（CSV 文件名）对应的会话。
    若不存在则返回空结构，便于创建新会话。
    """
    data = _load_sessions_raw()
    by_file = data.get("by_file", {})
    item = by_file.get(history_filename)
    if item is None:
        return {
            "id": str(uuid.uuid4()),
            "title": history_filename.replace(".csv", "")[:50],
            "messages": [],
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "data_snapshots_meta": [],
        }
    return item


def save_session_for_file(history_filename: str, session: Dict[str, Any]) -> None:
    """将会话写入 sessions.json，按 history_file 键存储"""
    data = _load_sessions_raw()
    by_file = data.get("by_file", {})
    session_id = session.get("id") or str(uuid.uuid4())
    session["id"] = session_id
    session["updated_at"] = datetime.now().isoformat()
    if not session.get("created_at"):
        session["created_at"] = datetime.now().isoformat()
    # 只持久化 messages 和快照元数据
    payload = {
        "id": session["id"],
        "title": session.get("title", history_filename.replace(".csv", "")[:50]),
        "messages": [_serialize_message(m) for m in session.get("messages", [])],
        "created_at": session["created_at"],
        "updated_at": session["updated_at"],
        "data_snapshots_meta": [_serialize_snapshot(s) for s in session.get("data_snapshots_meta", [])],
    }
    by_file[history_filename] = payload
    data["by_file"] = by_file
    data.setdefault("by_id", {})[session_id] = history_filename
    _save_sessions_raw(data)


def rename_session_file_key(old_history_filename: str, new_history_filename: str) -> bool:
    """历史文件重命名后，同步迁移 sessions.json 的键。"""
    if not old_history_filename or not new_history_filename:
        return False

    data = _load_sessions_raw()
    by_file = data.get("by_file", {})
    by_id = data.get("by_id", {})

    if old_history_filename not in by_file:
        return False

    item = by_file.pop(old_history_filename)
    by_file[new_history_filename] = item

    session_id = item.get("id")
    if session_id:
        by_id[session_id] = new_history_filename

    data["by_file"] = by_file
    data["by_id"] = by_id
    _save_sessions_raw(data)
    return True


def list_history_files_with_sessions() -> List[str]:
    """返回所有 CSV 历史文件列表（与 sessions 可能部分对应）"""
    _ensure_dir()
    files = [f for f in os.listdir(HISTORY_DIR) if f.endswith(".csv")]
    files.sort(reverse=True)
    return files
