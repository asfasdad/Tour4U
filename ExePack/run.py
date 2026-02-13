import sys
import os
import time
import webbrowser
import threading
import socket
from urllib import request
from streamlit.web import cli as stcli

def resolve_path(path):
    if getattr(sys, "frozen", False):
        basedir = getattr(sys, "_MEIPASS", os.path.dirname(sys.executable))
    else:
        basedir = os.path.dirname(__file__)
    return os.path.join(basedir, path)

def pick_server_port(default_port=8501):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("127.0.0.1", default_port))
            return default_port
        except OSError:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])


def wait_for_server(port, timeout_sec=45):
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


def open_browser(port):
    if wait_for_server(port):
        webbrowser.open(f"http://127.0.0.1:{port}")

if __name__ == "__main__":
    app_name = "main.py"
    app_path = resolve_path(app_name)
    port = pick_server_port(8501)
    
    # 【新增】启动一个独立的线程来执行“打开浏览器”的操作
    # 必须用线程，因为下面的 stcli.main() 会一直运行，卡住后面的代码
    threading.Thread(target=open_browser, args=(port,), daemon=True).start()
    print(f"[run] Starting UI at http://127.0.0.1:{port}")
    
    # 配置启动参数
    sys.argv = [
        "streamlit",
        "run",
        app_path,
        "--global.developmentMode=false",
        "--client.showErrorDetails=false",
        "--browser.gatherUsageStats=false",
        "--server.headless=true",  # 保持 headless 为 true，这更稳定，通过上面的线程来手动打开
        "--server.address=127.0.0.1",
        f"--server.port={port}",
    ]
    sys.exit(stcli.main())
