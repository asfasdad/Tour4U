# -*- coding: utf-8 -*-
"""Streamlit 打包入口脚本（CLI 模式）。"""
import sys
import os
import webbrowser
import threading
import time
from streamlit.web import cli as stcli


def _resolve_base_dir() -> str:
    if getattr(sys, "frozen", False):
        exe_dir = os.path.dirname(sys.executable)
        meipass = getattr(sys, "_MEIPASS", exe_dir)
        return exe_dir if os.path.exists(os.path.join(exe_dir, "main.py")) else meipass
    return os.path.dirname(os.path.abspath(__file__))


BASE_DIR = _resolve_base_dir()
sys.path.insert(0, BASE_DIR)


def _ensure_history_dir() -> None:
    if getattr(sys, "frozen", False):
        data_dir = os.path.join(os.path.dirname(sys.executable), "history_data")
    else:
        data_dir = os.path.join(BASE_DIR, "history_data")
    os.makedirs(data_dir, exist_ok=True)


def _open_browser() -> None:
    time.sleep(2)
    try:
        webbrowser.open("http://localhost:8501")
    except Exception:
        pass


def main() -> int:
    _ensure_history_dir()
    main_script = os.path.join(BASE_DIR, "main.py")

    threading.Thread(target=_open_browser, daemon=True).start()
    sys.argv = [
        "streamlit",
        "run",
        main_script,
        "--global.developmentMode=false",
        "--client.showErrorDetails=false",
        "--browser.gatherUsageStats=false",
        "--server.headless=true",
        "--server.enableCORS=false",
        "--server.enableXsrfProtection=false",
        "--server.address=localhost",
        "--server.port=8501",
    ]
    return int(stcli.main())


if __name__ == "__main__":
    raise SystemExit(main())
