# -*- coding: utf-8 -*-
"""
PyInstaller spec 文件用于打包 Streamlit 应用
使用方法: pyinstaller ExePack.spec --clean
"""
import sys
import os
from PyInstaller.building.build_main import Analysis, PYZ, EXE, COLLECT
from PyInstaller.utils.hooks import copy_metadata, collect_data_files

# 基础目录 - 使用 SPECPATH 变量（PyInstaller 提供的 spec 文件所在目录）
BASE_DIR = os.path.abspath(SPECPATH)

# 分析依赖
a = Analysis(
    ['launcher.py'],
    pathex=[BASE_DIR],
    binaries=[],
    datas=[
        # 主应用文件
        ('main.py', '.'),
        ('run.py', '.'),
        ('data_diff.py', '.'),
        ('llm_providers.py', '.'),
        ('session_store.py', '.'),
        # Streamlit 静态资源（防止回退到 Node dev server: 3000，导致 / 404）
        *collect_data_files('streamlit'),
        # Streamlit 元数据（修复 importlib.metadata.PackageNotFoundError）
        copy_metadata('streamlit')[0],
        copy_metadata('pandas')[0],
        copy_metadata('altair')[0],
    ],
    hiddenimports=[
        # Streamlit 隐藏导入
        'streamlit',
        'streamlit.web.cli',
        'streamlit.runtime.scriptrunner',
        'streamlit.runtime.scriptrunner.magic_funcs',
        'streamlit.runtime.uploaded_file_manager',
        'streamlit.runtime.media_file_manager',
        'streamlit.watcher',
        'streamlit.components.v1',
        'streamlit.elements',
        'streamlit.elements.image',
        
        # 项目依赖
        'pandas',
        'altair',
        'requests',
        'DrissionPage',
        'pydantic',
        
        # 数据库
        'sqlite3',
        
        # 其他可能需要的
        'packaging',
        'packaging.version',
        'packaging.specifiers',
        'packaging.requirements',
        'watchdog',
        'tornado',
        'tornado.ioloop',
        'tornado.web',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # 排除不需要的大包，减少体积
        'matplotlib',
        'tkinter',
        'PyQt5',
        'PyQt6',
        'PySide2',
        'PySide6',
        'test',
        'unittest',
        'doctest',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=None,
    noarchive=False,
)

# 打包为 PYZ
pyz = PYZ(a.pure, a.zipped_data, cipher=None)

# 创建 EXE
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='竞品分析工具',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,  # 使用 UPX 压缩（如果安装了 UPX）
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,  # 显示控制台窗口（便于调试，可改为 False）
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    # Windows 图标（如有）
    # icon='icon.ico',
)

# onedir 模式：把依赖/数据收集到一个文件夹里（更稳定，避免 onefile 解压/路径导致的 404）
coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='竞品分析工具',
)
