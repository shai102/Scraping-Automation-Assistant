import logging

from core.services.naming_templates import render_filename_template
from utils.library_paths import build_existing_library_target
from utils.value_utils import safe_str


def prefer_existing_library_target(gui, item, new_name, metadata):
    preserve_var = getattr(gui, "preserve_existing_folder", None)
    if preserve_var is None:
        return ""
    getter = getattr(preserve_var, "get", None)
    preserve_enabled = bool(getter()) if callable(getter) else bool(preserve_var)
    if not preserve_enabled:
        return ""
    return build_existing_library_target(item.path, new_name, metadata)


def render_media_filename(gui, template, **kwargs):
    renderer = getattr(gui, "_render_media_filename", None)
    if callable(renderer):
        return renderer(template, **kwargs)

    context = {
        "title": safe_str(kwargs.get("title", "")),
        "year": safe_str(kwargs.get("year", "")),
        "season": safe_str(kwargs.get("season", "")),
        "episode": safe_str(kwargs.get("episode", "")),
        "ep_name": safe_str(kwargs.get("ep_name", "")),
        "ext": safe_str(kwargs.get("ext", "")),
        "media_suffix": "",
        "original_title": safe_str(kwargs.get("original_title", "")),
        "rating": kwargs.get("rating") or 0,
        "genres": kwargs.get("genres") or [],
        "studios": kwargs.get("studios") or [],
        "overview": safe_str(kwargs.get("overview", "")),
        "ep_plot": safe_str(kwargs.get("ep_plot", "")),
        "release": safe_str(kwargs.get("release", "")),
    }
    return render_filename_template(template, context, False), ""


def mark_skipped_recap(gui, item, title, reason):
    item.metadata = {
        "id": "None",
        "parse_source": str(getattr(item, "parse_source", "") or ""),
        "skip_reason": reason,
    }
    item.new_name_only = ""
    item.full_target = ""
    item.parse_source = str(getattr(item, "parse_source", "") or "")
    gui.root.after(
        0,
        lambda: gui.update_item_display(
            item,
            title=title or "已跳过",
            match_id="None",
            target="(总集篇已跳过)",
            status="已跳过(总集篇)",
        ),
    )


def notify_error(gui, title, message):
    handler = getattr(gui, "show_error", None)
    if callable(handler):
        try:
            handler(title, message)
            return
        except Exception:
            pass
    logging.error("%s: %s", title, message)
