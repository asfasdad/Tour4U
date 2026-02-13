@echo off
chcp 65001 >nul
echo ============================================
echo  竞品分析工具 - EXE 打包脚本
echo ============================================
echo.

REM 检查虚拟环境
if not exist ".venv\Scripts\python.exe" (
    echo [错误] 虚拟环境不存在: .venv\Scripts\python.exe
    echo 请先创建虚拟环境并安装依赖
    pause
    exit /b 1
)

echo [1/4] 检查 PyInstaller...
.\.venv\Scripts\python.exe -c "import PyInstaller" 2>nul
if errorlevel 1 (
    echo 安装 PyInstaller...
    .\.venv\Scripts\python.exe -m pip install pyinstaller
)

echo [2/4] 清理旧构建...
if exist "build" rmdir /s /q "build"
if exist "dist" rmdir /s /q "dist"

echo [3/4] 开始打包（这可能需要几分钟）...
.\.venv\Scripts\python.exe -m PyInstaller ExePack.spec --clean --noconfirm
if errorlevel 1 (
    echo [错误] 打包失败
    pause
    exit /b 1
)

echo [4/4] 打包完成！
echo.
echo 输出文件: dist\竞品分析工具.exe
echo.
pause
