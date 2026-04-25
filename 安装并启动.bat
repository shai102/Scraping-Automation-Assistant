@echo off
chcp 65001 >nul
echo ============================================
echo      刮削助手 — 安装启动器
echo ============================================
echo.

:: 检查 Python
python --version >nul 2>&1
if errorlevel 1 (
    echo [错误] 未检测到 Python，请先安装 Python 3.10 或更高版本
    echo        下载地址: https://www.python.org/downloads/
    pause
    exit /b 1
)

echo [1/2] 正在安装依赖包（首次运行需要联网）...
pip install -r requirements.txt -q
if errorlevel 1 (
    echo [错误] 依赖安装失败，请检查网络连接
    pause
    exit /b 1
)

echo [2/2] 启动程序...
echo.
python main.py
pause
