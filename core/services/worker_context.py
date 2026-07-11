"""WorkerContext — A GUI-free runtime context that task_runner / execution_runner
can consume directly. It reads configuration from renamer_config.json and exposes
the same attribute/method surface that the worker modules access on the ``gui`` parameter.

All UI-bound callbacks (tree updates, progress bar, messagebox) are replaced with
no-ops or optional callback hooks that the web layer can subscribe to.
"""

import threading
from typing import Any, Callable, Dict, Optional

from core.services.worker_context_config import (
    apply_runtime_config,
    clamp_temperature,
    clamp_top_p,
    clamp_workers,
    get_ai_temperature,
    get_ai_top_p,
    get_execution_workers,
    get_preview_workers,
    get_sync_workers,
    get_symlink_export_workers,
    load_config_from_disk,
)
from core.services.worker_context_media_mixin import WorkerContextMediaMixin
from core.services.worker_context_runtime_mixin import WorkerContextRuntimeMixin
from core.services.worker_context_stubs import (
    DummyButton,
    DummyLabel,
    DummyProgressbar,
    DummyRoot,
    DummyTree,
    SimpleVar,
)
from utils.media_defaults import (
    DEFAULT_LANG_TAGS,
    DEFAULT_MOVIE_FORMAT,
    DEFAULT_SUB_AUDIO_EXTS,
    DEFAULT_TV_FORMAT,
    DEFAULT_VIDEO_EXTS,
)


class WorkerContext(WorkerContextMediaMixin, WorkerContextRuntimeMixin):
    """Runtime context for the worker modules.

    All tkinter-specific concepts (root.after, tree, pbar, etc.) are replaced with
    no-ops or callback hooks.
    """

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(
        self,
        config: Optional[dict] = None,
        on_status: Optional[Callable[[int, str, dict], None]] = None,
    ):
        """
        Parameters
        ----------
        config : dict, optional
            Overrides for renamer_config.json.  If *None*, the file is loaded from disk.
        on_status : callable(record_id, status_text, extra_dict), optional
            Hook invoked whenever a file's processing status changes.
        """
        if config is None:
            config = self._load_config_from_disk()
        self._cfg = config
        self._on_status = on_status

        # --- Locks & caches expected by the worker modules ---
        self.cache_lock = threading.Lock()
        self.file_write_lock = threading.Lock()
        self.popup_lock = threading.Lock()
        self.preview_skip_all_event = threading.Event()
        self.preview_skip_dirs: set = set()
        self.dir_cache: Dict[str, Any] = {}
        self.db_cache: Dict[str, Any] = {}
        self.manual_locks: Dict[str, Any] = {}
        self.forced_seasons: Dict[str, int] = {}
        self.forced_offsets: Dict[str, int] = {}
        self.dir_parse_events: Dict[str, threading.Event] = {}
        self.db_resolution_events: Dict[str, threading.Event] = {}
        self.embedding_cache: Dict[str, Any] = {}
        self.ollama_embed_endpoint: Optional[str] = None

        # --- Config vars (expose via SimpleVar.get()) ---
        self.sf_api_key = SimpleVar(config.get("sf_api_key", ""))
        self.sf_api_url = SimpleVar(config.get("sf_api_url", "https://api.siliconflow.cn/v1"))
        self.sf_model = SimpleVar(config.get("sf_model", "deepseek-ai/DeepSeek-V3"))
        self.ai_temperature = SimpleVar(f"{self._clamp_temperature(config.get('ai_temperature'), 0.2):.2f}")
        self.ai_top_p = SimpleVar(f"{self._clamp_top_p(config.get('ai_top_p'), 0.85):.2f}")
        self.bgm_api_key = SimpleVar(config.get("bgm_api_key", ""))
        self.tmdb_api_key = SimpleVar(config.get("tmdb_api_key", ""))
        self.tv_format = SimpleVar(config.get("tv_format", DEFAULT_TV_FORMAT))
        self.movie_format = SimpleVar(config.get("movie_format", DEFAULT_MOVIE_FORMAT))
        self.video_exts = SimpleVar(config.get("video_exts", DEFAULT_VIDEO_EXTS))
        self.sub_audio_exts = SimpleVar(config.get("sub_audio_exts", DEFAULT_SUB_AUDIO_EXTS))
        self.lang_tags = SimpleVar(config.get("lang_tags", DEFAULT_LANG_TAGS))
        self.preserve_media_suffix = SimpleVar(config.get("preserve_media_suffix", False))
        self.ollama_url = SimpleVar(config.get("ollama_url", "http://localhost:11434"))
        self.ollama_model = SimpleVar(config.get("ollama_model", ""))
        self.embedding_model = SimpleVar(config.get("embedding_model", ""))
        self.embedding_source = SimpleVar(config.get("embedding_source", "local"))
        self.online_embedding_model = SimpleVar(config.get("online_embedding_model", ""))
        self.prefer_ollama = SimpleVar(config.get("prefer_ollama", False))
        self.use_embedding_rank = SimpleVar(config.get("use_embedding_rank", True))
        self.ai_mode = SimpleVar(config.get("ai_mode", "assist"))  # disabled / assist / force
        self.preview_workers = SimpleVar(str(self._clamp_workers(config.get("preview_workers"), 1)))
        self.symlink_export_workers = SimpleVar(str(self._clamp_workers(config.get("symlink_export_workers"), 3)))
        self.sync_workers = SimpleVar(str(self._clamp_workers(config.get("sync_workers"), 5)))
        self.execution_workers = SimpleVar(str(self._clamp_workers(config.get("execution_workers"), 5)))
        self.media_type_override = SimpleVar(config.get("media_type_override", "自动判断"))
        self.target_root = SimpleVar(config.get("target_root", ""))
        self.source_var = SimpleVar(config.get("data_source", "siliconflow_tmdb"))
        self.preserve_existing_folder = SimpleVar(config.get("preserve_existing_folder", False))
        self.strip_keywords = config.get("strip_keywords", [])

        # --- File list (populated externally) ---
        self.file_list: list = []

        # --- Dummy UI stubs (task_runner calls gui.root.after, gui.tree, etc.) ---
        self.root = DummyRoot(self)
        self.tree = DummyTree(self)
        self.pbar = DummyProgressbar()
        self.status = DummyLabel()
        self.btn_pre = DummyButton()
        apply_runtime_config(self, config)

    def emit_status(self, record_id, status_text: str, extra: Optional[dict] = None):
        """Headless status event used while legacy worker UI calls are phased out."""
        if not callable(self._on_status):
            return
        try:
            self._on_status(record_id, status_text, dict(extra or {}))
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Config helpers used by the worker modules
    # ------------------------------------------------------------------

    @staticmethod
    def _load_config_from_disk() -> dict:
        return load_config_from_disk()

    def reload_config(self):
        cfg = self._load_config_from_disk()
        apply_runtime_config(self, cfg)

    @staticmethod
    def _clamp_workers(value, default):
        return clamp_workers(value, default)

    @staticmethod
    def _clamp_temperature(value, default=0.2):
        return clamp_temperature(value, default)

    @staticmethod
    def _clamp_top_p(value, default=0.9):
        return clamp_top_p(value, default)

    def _get_ai_temperature(self):
        return get_ai_temperature(self)

    def _get_ai_top_p(self):
        return get_ai_top_p(self)

    def _get_preview_workers(self):
        return get_preview_workers(self)

    def _get_symlink_export_workers(self):
        return get_symlink_export_workers(self)

    def _get_sync_workers(self):
        return get_sync_workers(self)

    def _get_execution_workers(self):
        return get_execution_workers(self)
