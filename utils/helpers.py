"""Legacy compatibility shim.

This module used to contain a large mix of runtime constants, parsing helpers,
cache/proxy helpers, and metadata utilities. The implementation has been
split into focused modules. Keep re-exporting the historical names here so
older imports continue to work while new code depends on the new modules
directly.
"""

from core.metadata.completeness import metadata_is_incomplete, metadata_missing_fields
from core.metadata.sidecar_service import _nfo_has_empty_plot, save_image, write_nfo
from utils.app_runtime import (
    CONFIG_FILE,
    TIMEOUT_AI_CHAT,
    TIMEOUT_AI_TEST,
    TIMEOUT_DB_DETAIL,
    TIMEOUT_DB_SEARCH,
    TIMEOUT_IMAGE_DOWNLOAD,
    TIMEOUT_OLLAMA_CHAT,
    TIMEOUT_OLLAMA_EMBED,
    TIMEOUT_OLLAMA_TAGS,
    USER_AGENT,
)
from utils.cache import (
    CACHE_EXPIRY_DAYS,
    CACHE_FILE,
    bypass_api_cache,
    cached_request,
    clear_api_cache_file,
    flush_api_cache,
    get_cache_key,
    invalidate_cache_prefix,
    load_cache,
    save_cache,
    set_cache_expiry_days,
)
from utils.candidate_utils import candidate_to_result, format_candidate_label
from utils.error_utils import (
    ERROR_CODES,
    ERROR_CODE_CONFIG,
    ERROR_CODE_HTTP,
    ERROR_CODE_INVALID,
    ERROR_CODE_NO_RESULT,
    ERROR_CODE_PARSE,
    ERROR_CODE_TIMEOUT,
    ERROR_CODE_UNKNOWN,
    format_error_message,
    parse_error_message,
)
from utils.library_paths import build_existing_library_target, extract_db_id_from_path
from utils.media_defaults import (
    DEFAULT_LANG_TAGS,
    DEFAULT_MOVIE_FORMAT,
    DEFAULT_SUB_AUDIO_EXTS,
    DEFAULT_TV_FORMAT,
    DEFAULT_VIDEO_EXTS,
)
from utils.media_patterns import VERSION_TAG_RE
from utils.proxy import (
    DEFAULT_NO_PROXY,
    ProxyAwareSession,
    apply_proxy_environment,
    create_retry_session,
    normalize_proxy_url,
    override_proxy_config,
    proxy_bypass_url,
    proxy_summary,
    request_get,
    request_post,
    request_proxy_kwargs,
    session,
)
from utils.title_parsing import (
    BRACKET_CONTENT_RE,
    BRACKET_NOISE_RE,
    EPISODE_NOISE_NUMBERS,
    EXTRA_TITLE_MARKER_RE,
    GENERIC_SEASON_TITLE_RE,
    GROUP_RELEASE_BRACKET_RE,
    INVALID_QUERY_TITLES,
    INVALID_QUERY_TITLES_NORMALIZED,
    LANG_TAG_COMBO_RE,
    LANG_TAG_TOKEN_RE,
    LEADING_RELEASE_GROUP_RE,
    MEDIA_NOISE_TOKEN_RE,
    PLATFORM_PREFIX_RE,
    QUERY_SEASON_EP_RE,
    VARIANT_TITLE_MARKERS,
    build_db_query_plan,
    build_fallback_token_queries,
    build_query_titles,
    candidate_looks_like_extra_title,
    candidate_looks_like_unrequested_variant,
    clean_search_title,
    derive_title_from_filename,
    extract_bracket_title_from_filename,
    extract_episode_number,
    extract_title_after_leading_release_group,
    is_decimal_episode,
    is_meaningful_query_title,
    normalize_search_query_title,
    split_mixed_title,
    strip_platform_prefix,
    text_mentions_extra_title,
    title_variant_markers,
    unique_keep_order,
)
from utils.value_utils import (
    extract_year_from_release,
    normalize_compare_text,
    normalize_parse_source,
    safe_filename,
    safe_int,
    safe_str,
)


def center_window(window, parent, width, height):
    parent.update_idletasks()
    window.update_idletasks()

    parent_w = parent.winfo_width()
    parent_h = parent.winfo_height()
    if parent_w <= 1 or parent_h <= 1:
        parent_w = parent.winfo_screenwidth()
        parent_h = parent.winfo_screenheight()

    parent_x = parent.winfo_rootx()
    parent_y = parent.winfo_rooty()

    x = parent_x + (parent_w // 2) - (width // 2)
    y = parent_y + (parent_h // 2) - (height // 2)
    x = max(0, x)
    y = max(0, y)
    window.geometry(f"{width}x{height}+{x}+{y}")


__all__ = [
    "USER_AGENT",
    "CONFIG_FILE",
    "CACHE_FILE",
    "CACHE_EXPIRY_DAYS",
    "TIMEOUT_IMAGE_DOWNLOAD",
    "TIMEOUT_DB_SEARCH",
    "TIMEOUT_DB_DETAIL",
    "TIMEOUT_AI_CHAT",
    "TIMEOUT_AI_TEST",
    "TIMEOUT_OLLAMA_TAGS",
    "TIMEOUT_OLLAMA_CHAT",
    "TIMEOUT_OLLAMA_EMBED",
    "DEFAULT_TV_FORMAT",
    "DEFAULT_MOVIE_FORMAT",
    "DEFAULT_VIDEO_EXTS",
    "DEFAULT_SUB_AUDIO_EXTS",
    "DEFAULT_LANG_TAGS",
    "DEFAULT_NO_PROXY",
    "LANG_TAG_TOKEN_RE",
    "LANG_TAG_COMBO_RE",
    "INVALID_QUERY_TITLES",
    "INVALID_QUERY_TITLES_NORMALIZED",
    "GENERIC_SEASON_TITLE_RE",
    "BRACKET_CONTENT_RE",
    "GROUP_RELEASE_BRACKET_RE",
    "LEADING_RELEASE_GROUP_RE",
    "BRACKET_NOISE_RE",
    "QUERY_SEASON_EP_RE",
    "EXTRA_TITLE_MARKER_RE",
    "VARIANT_TITLE_MARKERS",
    "VERSION_TAG_RE",
    "PLATFORM_PREFIX_RE",
    "strip_platform_prefix",
    "EPISODE_NOISE_NUMBERS",
    "MEDIA_NOISE_TOKEN_RE",
    "ERROR_CODE_TIMEOUT",
    "ERROR_CODE_CONFIG",
    "ERROR_CODE_HTTP",
    "ERROR_CODE_PARSE",
    "ERROR_CODE_NO_RESULT",
    "ERROR_CODE_INVALID",
    "ERROR_CODE_UNKNOWN",
    "ERROR_CODES",
    "format_error_message",
    "parse_error_message",
    "safe_filename",
    "normalize_compare_text",
    "extract_year_from_release",
    "format_candidate_label",
    "candidate_to_result",
    "center_window",
    "clean_search_title",
    "is_meaningful_query_title",
    "extract_title_after_leading_release_group",
    "extract_bracket_title_from_filename",
    "unique_keep_order",
    "normalize_search_query_title",
    "build_fallback_token_queries",
    "is_decimal_episode",
    "extract_episode_number",
    "derive_title_from_filename",
    "text_mentions_extra_title",
    "title_variant_markers",
    "candidate_looks_like_extra_title",
    "candidate_looks_like_unrequested_variant",
    "split_mixed_title",
    "build_query_titles",
    "build_db_query_plan",
    "normalize_parse_source",
    "safe_str",
    "safe_int",
    "extract_db_id_from_path",
    "build_existing_library_target",
    "metadata_is_incomplete",
    "metadata_missing_fields",
    "_nfo_has_empty_plot",
    "save_image",
    "write_nfo",
    "bypass_api_cache",
    "cached_request",
    "clear_api_cache_file",
    "flush_api_cache",
    "get_cache_key",
    "invalidate_cache_prefix",
    "load_cache",
    "save_cache",
    "set_cache_expiry_days",
    "ProxyAwareSession",
    "apply_proxy_environment",
    "create_retry_session",
    "normalize_proxy_url",
    "override_proxy_config",
    "proxy_bypass_url",
    "proxy_summary",
    "request_get",
    "request_post",
    "request_proxy_kwargs",
    "session",
]
