import logging
from utils.cache import set_cache_expiry_days
from utils.value_utils import safe_int


def load_config_from_disk() -> dict:
    try:
        from core.settings.config_service import load_settings
        return load_settings()
    except Exception as err:
        logging.error("WorkerContext: 加载配置失败: %s", err)
        return {}


def clamp_workers(value, default):
    num = safe_int(value, default)
    return max(1, min(10, num))


def clamp_temperature(value, default=0.2):
    try:
        return max(0.0, min(2.0, float(value)))
    except (TypeError, ValueError):
        return float(default)


def clamp_top_p(value, default=0.9):
    try:
        return max(0.0, min(1.0, float(value)))
    except (TypeError, ValueError):
        return float(default)


def apply_runtime_config(ctx, cfg: dict):
    ctx._cfg = cfg
    for attr in (
        "sf_api_key",
        "sf_api_url",
        "sf_model",
        "bgm_api_key",
        "tmdb_api_key",
        "ollama_url",
        "ollama_model",
        "embedding_model",
        "embedding_source",
        "online_embedding_model",
        "tv_format",
        "movie_format",
        "video_exts",
        "sub_audio_exts",
        "lang_tags",
    ):
        var = getattr(ctx, attr, None)
        if var is not None:
            var.set(cfg.get(attr, var.get()))

    ctx.preserve_media_suffix.set(cfg.get("preserve_media_suffix", False))
    ctx.prefer_ollama.set(cfg.get("prefer_ollama", False))
    ctx.use_embedding_rank.set(cfg.get("use_embedding_rank", True))
    ctx.ai_mode.set(cfg.get("ai_mode", "assist"))
    ctx.symlink_export_workers.set(str(clamp_workers(cfg.get("symlink_export_workers"), 3)))
    ctx.target_root.set(cfg.get("target_root", ctx.target_root.get()))
    ctx.source_var.set(cfg.get("data_source", ctx.source_var.get()))
    ctx.strip_keywords = cfg.get("strip_keywords", [])
    set_cache_expiry_days(cfg.get("cache_expiry_days", 7))


def get_ai_temperature(ctx):
    return clamp_temperature(ctx.ai_temperature.get(), 0.2)


def get_ai_top_p(ctx):
    return clamp_top_p(ctx.ai_top_p.get(), 0.85)


def get_preview_workers(ctx):
    return clamp_workers(ctx.preview_workers.get(), 1)


def get_symlink_export_workers(ctx):
    return clamp_workers(ctx.symlink_export_workers.get(), 3)


def get_sync_workers(ctx):
    return clamp_workers(ctx.sync_workers.get(), 5)


def get_execution_workers(ctx):
    return clamp_workers(ctx.execution_workers.get(), 5)


def get_media_exts(ctx):
    video_exts = [e.strip().lower() for e in ctx.video_exts.get().split(",") if e.strip()]
    sub_exts = [e.strip().lower() for e in ctx.sub_audio_exts.get().split(",") if e.strip()]
    return tuple(video_exts + sub_exts)


def get_sub_audio_exts(ctx):
    return tuple(e.strip().lower() for e in ctx.sub_audio_exts.get().split(",") if e.strip())
