@echo off
chcp 65001 >nul
echo ============================================
echo      正在打包 刮削助手 v2.0
echo ============================================

:: 0. 生成 ICO 图标（与托盘图标一致的蓝色圆形）
python gen_ico.py
echo.

:: 1. 执行打包
pyinstaller --noconfirm ^
  --onefile ^
  --windowed ^
  --name "刮削助手" ^
  --icon "app.ico" ^
  --collect-all guessit ^
  --collect-all babelfish ^
  --collect-all Pillow ^
  --collect-all pystray ^
  --collect-all uvicorn ^
  --collect-all fastapi ^
  --collect-all sqlalchemy ^
  --collect-all watchdog ^
  --collect-all starlette ^
  --collect-all aiofiles ^
  --collect-all anyio ^
  --collect-all h11 ^
  --collect-all websockets ^
  --hidden-import uvicorn.logging ^
  --hidden-import uvicorn.loops ^
  --hidden-import uvicorn.loops.auto ^
  --hidden-import uvicorn.protocols ^
  --hidden-import uvicorn.protocols.http ^
  --hidden-import uvicorn.protocols.http.auto ^
  --hidden-import uvicorn.protocols.websockets ^
  --hidden-import uvicorn.protocols.websockets.auto ^
  --hidden-import uvicorn.lifespan ^
  --hidden-import uvicorn.lifespan.on ^
  --hidden-import sqlalchemy.dialects.sqlite ^
  --add-data "web/dist;web/dist" ^
  --add-data "ai;ai" ^
  --add-data "core;core" ^
  --add-data "db;db" ^
  --add-data "api;api" ^
  --add-data "monitor;monitor" ^
  --add-data "utils;utils" ^
  --clean ^
  main.py

echo.
echo --------------------------------------------
echo 正在清理临时文件...

:: 2. 清理
rd /s /q build
del /q "刮削助手.spec"

echo.
echo [完成] 临时文件已清理
echo [提示] 请在 dist 文件夹中查看生成的 EXE 文件
echo        运行 EXE 后浏览器会自动打开管理页面
echo --------------------------------------------
pause