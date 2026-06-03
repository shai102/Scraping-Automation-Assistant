import os
import sys


USER_AGENT = "MyMediaRenamer/73.0 (Fully Customizable Edition)"


def resolve_data_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.environ.get("DATA_DIR") or os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
    )


DATA_DIR = resolve_data_dir()
CONFIG_FILE = os.path.join(DATA_DIR, "renamer_config.json")

TIMEOUT_IMAGE_DOWNLOAD = (10, 30)
TIMEOUT_DB_SEARCH = (6, 20)
TIMEOUT_DB_DETAIL = (8, 25)
TIMEOUT_AI_CHAT = (10, 50)
TIMEOUT_AI_TEST = (8, 25)
TIMEOUT_OLLAMA_TAGS = (4, 12)
TIMEOUT_OLLAMA_CHAT = (8, 45)
TIMEOUT_OLLAMA_EMBED = (8, 30)
