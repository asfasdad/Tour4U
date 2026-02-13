from PyInstaller.utils.hooks import copy_metadata, collect_data_files

# 1. 复制基础元数据
datas = copy_metadata("streamlit")

# 2. 搬运网页界面文件
datas += collect_data_files("streamlit")

# 3. 强制包含容易丢失的模块
hiddenimports = [
    "streamlit.runtime.scriptrunner.magic_funcs",
    "streamlit.runtime.scriptrunner.script_runner",
    "streamlit.web.server.server",
]