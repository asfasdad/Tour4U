import sys
import os
import time
import webbrowser
import threading
from streamlit.web import cli as stcli

def resolve_path(path):
    if getattr(sys, "frozen", False):
        basedir = sys._MEIPASS
    else:
        basedir = os.path.dirname(__file__)
    return os.path.join(basedir, path)

# 【新增】定义一个自动打开浏览器的函数
def open_browser():
    # 等待 2 秒，确保 Streamlit 服务器已经启动
    time.sleep(2)
    # 调用系统默认浏览器打开网页
    webbrowser.open("http://localhost:8501")

if __name__ == "__main__":
    app_name = "main.py"
    app_path = resolve_path(app_name)
    
    # 【新增】启动一个独立的线程来执行“打开浏览器”的操作
    # 必须用线程，因为下面的 stcli.main() 会一直运行，卡住后面的代码
    threading.Thread(target=open_browser, daemon=True).start()
    
    # 配置启动参数
    sys.argv = [
        "streamlit",
        "run",
        app_path,
        "--global.developmentMode=false",
        "--client.showErrorDetails=false",
        "--browser.gatherUsageStats=false",
        "--server.headless=true",  # 保持 headless 为 true，这更稳定，通过上面的线程来手动打开
        "--server.address=localhost",
        "--server.port=8501",
    ]
    sys.exit(stcli.main())
