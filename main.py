import logging
import os
import sys
import threading
import webbrowser


# When frozen with --windowed, sys.stdout/stderr are None, which causes
# uvicorn's logging to crash on sys.stdout.isatty().  Redirect them to the
# log file so everything is captured and the crash is avoided.
def _fix_frozen_stdio():
    if not getattr(sys, "frozen", False):
        return
    log_path = _resolve_log_path()
    _fh = open(log_path, "a", encoding="utf-8", buffering=1)
    if sys.stdout is None:
        sys.stdout = _fh
    if sys.stderr is None:
        sys.stderr = _fh


def _resolve_log_path():
    if getattr(sys, "frozen", False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, "media_renamer.log")


def _setup_logging():
    log_path = _resolve_log_path()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
        force=True,
    )


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
    return pystray.Icon("刮削助手", img, "刮削助手 v2.0", menu)


def _run_server():
    import uvicorn
    if getattr(sys, "frozen", False):
        from server import app as _app
        uvicorn.run(_app, host=HOST, port=PORT, log_level="info")
    else:
        uvicorn.run("server:app", host=HOST, port=PORT, log_level="info", reload=False)


def main():
    _fix_frozen_stdio()   # must be first, before any uvicorn import
    _setup_logging()

    print(f"\n  刮削助手 v2.0")
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
