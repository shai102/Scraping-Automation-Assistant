import logging
import os
import sys
import threading
import webbrowser


# When frozen with --windowed, sys.stdout/stderr are None, which causes
# uvicorn's logging to crash on sys.stdout.isatty().  Create dummy streams
# to avoid the crash, but don't write to the log file.
def _fix_frozen_stdio():
    if not getattr(sys, "frozen", False):
        return
    import io
    # Create a dummy stream that discards all output
    class NullWriter(io.StringIO):
        def write(self, s):
            pass
    if sys.stdout is None:
        sys.stdout = NullWriter()
    if sys.stderr is None:
        sys.stderr = NullWriter()


def _resolve_log_path():
    if getattr(sys, "frozen", False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, "media_renamer.log")


def _setup_logging():
    log_path = _resolve_log_path()

    # 创建一个自定义过滤器，只允许 ERROR 及以上级别通过
    class ErrorOnlyFilter(logging.Filter):
        def filter(self, record):
            return record.levelno >= logging.ERROR

    # 获取根日志记录器
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # 清除现有处理器
    root_logger.handlers.clear()

    # 创建文件处理器，只记录ERROR及以上级别
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.ERROR)
    file_handler.addFilter(ErrorOnlyFilter())
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    # 创建控制台处理器，记录INFO及以上级别
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    # 添加处理器到根日志记录器
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)


HOST = "0.0.0.0"
PORT = 8090


def _build_tray_icon():
    """Create a pystray Icon with a right-click menu."""
    from PIL import Image, ImageDraw
    import pystray

    # Draw a simple 64x64 icon (film-reel style)
    size = 64
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    # Outer circle
    draw.ellipse([2, 2, size - 3, size - 3], fill="#4361ee")
    # Inner circle
    draw.ellipse([18, 18, size - 19, size - 19], fill="#ffffff")
    # Center dot
    draw.ellipse([28, 28, size - 29, size - 29], fill="#4361ee")

    def on_open(_icon, _item):
        webbrowser.open(f"http://127.0.0.1:{PORT}")

    def on_quit(_icon, _item):
        _icon.visible = False
        _icon.stop()
        os._exit(0)

    menu = pystray.Menu(
        pystray.MenuItem("打开管理界面", on_open, default=True),
        pystray.Menu.SEPARATOR,
        pystray.MenuItem("退出", on_quit),
    )
    return pystray.Icon("刮削助手", img, "刮削助手 v2.2", menu)


def _run_server():
    import uvicorn

    # 创建一个自定义过滤器，只允许 ERROR 及以上级别写入文件
    class ErrorOnlyFilter(logging.Filter):
        def filter(self, record):
            return record.levelno >= logging.ERROR

    log_path = _resolve_log_path()

    # 创建自定义日志配置字典
    log_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "%(asctime)s - %(levelname)s - %(message)s",
            },
        },
        "filters": {
            "error_only": {
                "()": lambda: ErrorOnlyFilter(),
            },
        },
        "handlers": {
            "file": {
                "class": "logging.FileHandler",
                "level": "ERROR",
                "formatter": "default",
                "filename": log_path,
                "encoding": "utf-8",
                "filters": ["error_only"],
            },
        },
        "loggers": {
            "uvicorn": {
                "handlers": ["file"],
                "level": "INFO",
                "propagate": False,
            },
            "uvicorn.access": {
                "handlers": ["file"],
                "level": "INFO",
                "propagate": False,
            },
            "uvicorn.error": {
                "handlers": ["file"],
                "level": "INFO",
                "propagate": False,
            },
        },
        "root": {
            "handlers": ["file"],
            "level": "INFO",
        },
    }

    if getattr(sys, "frozen", False):
        from server import app as _app
        uvicorn.run(_app, host=HOST, port=PORT, log_config=log_config)
    else:
        uvicorn.run("server:app", host=HOST, port=PORT, reload=False, log_config=log_config)


def main():
    _fix_frozen_stdio()   # must be first, before any uvicorn import
    _setup_logging()

    print(f"\n  刮削助手 v2.2")
    print(f"  Web 管理界面: http://127.0.0.1:{PORT}\n")

    # Start uvicorn in a daemon thread so the main thread is free for the tray
    server_thread = threading.Thread(target=_run_server, daemon=True)
    server_thread.start()

    # Auto-open browser once the server is up
    threading.Timer(1.5, lambda: webbrowser.open(f"http://127.0.0.1:{PORT}")).start()

    # Run system tray icon on the main thread (required by pystray on Windows)
    try:
        icon = _build_tray_icon()
        icon.run()
    except Exception:
        # Fallback if pystray is unavailable: just wait for the server thread
        server_thread.join()


if __name__ == "__main__":
    main()
