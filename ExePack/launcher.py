# -*- coding: utf-8 -*-
"""Streamlit 打包入口脚本（CLI 模式）。"""
import sys
import os
import webbrowser
import threading
import time
import socket
from urllib import request
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


def _pick_server_port(default_port: int = 8501) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("127.0.0.1", default_port))
            return default_port
        except OSError:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])


def _wait_for_server(port: int, timeout_sec: int = 45) -> bool:
    url = f"http://127.0.0.1:{port}/_stcore/health"
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            with request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            time.sleep(0.4)
    return False


def _open_browser(port: int) -> None:
    if _wait_for_server(port):
        try:
            webbrowser.open(f"http://127.0.0.1:{port}")
        except Exception:
            pass


def main() -> int:
    _ensure_history_dir()
    main_script = os.path.join(BASE_DIR, "main.py")
    port = _pick_server_port(8501)

    threading.Thread(target=_open_browser, args=(port,), daemon=True).start()
    print(f"[launcher] Starting UI at http://127.0.0.1:{port}")
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
        "--server.address=127.0.0.1",
        f"--server.port={port}",
    ]
    return int(stcli.main())


if __name__ == "__main__":
    raise SystemExit(main())
