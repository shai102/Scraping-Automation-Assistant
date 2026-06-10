"""Docker entry point — runs uvicorn directly without pystray or webbrowser.

Usage:
    python docker_main.py

Environment variables:
    DATA_DIR   Path for persistent data (DB, config, cache, logs).
               Defaults to the project root when not set.
               In Docker, set to the mounted volume path (e.g. /data).
    PORT       HTTP port to listen on. Defaults to 8090.
    TZ         Timezone, e.g. Asia/Shanghai.
"""

import os
import sys

from utils.logging_setup import setup_logging


HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 8090))


def _resolve_data_dir() -> str:
    data_dir = os.environ.get("DATA_DIR")
    if data_dir:
        os.makedirs(data_dir, exist_ok=True)
        return data_dir
    return os.path.dirname(os.path.abspath(__file__))


def main() -> None:
    data_dir = _resolve_data_dir()
    setup_logging(data_dir, console_stream=sys.stdout)

    print("\n  刮削助手 (Docker Mode)")
    print(f"  DATA_DIR : {data_dir}")
    print(f"  Web 管理界面: http://0.0.0.0:{PORT}\n")

    import uvicorn

    uvicorn.run(
        "server:app",
        host=HOST,
        port=PORT,
        reload=False,
        log_level="info",
        log_config=None,
        access_log=False,
    )


if __name__ == "__main__":
    main()
