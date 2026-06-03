"""Compatibility facade for matching-related helpers.

The original matcher_service mixed three responsibilities:
- local Ollama filename parsing
- embedding fetch + candidate rerank
- candidate scoring and AI-assisted candidate picking

These are now split into focused service modules. Re-export the historical
functions here so existing imports continue to work.
"""

from core.services.candidate_picker_service import (
    auto_pick_candidate_by_score,
    pick_candidate_with_ollama,
    pick_candidate_with_openai_compatible,
    populate_candidate_listbox,
)
from core.services.embedding_service import (
    get_embedding,
    get_online_embedding,
    rerank_candidates_with_embedding,
)
from core.services.ollama_service import (
    extract_ollama_model_names,
    list_ollama_models,
    normalize_temperature as _normalize_temperature,
    normalize_top_p as _normalize_top_p,
    ollama_post_json,
    parse_with_ollama,
)


__all__ = [
    "auto_pick_candidate_by_score",
    "extract_ollama_model_names",
    "get_embedding",
    "get_online_embedding",
    "list_ollama_models",
    "ollama_post_json",
    "parse_with_ollama",
    "pick_candidate_with_ollama",
    "pick_candidate_with_openai_compatible",
    "populate_candidate_listbox",
    "rerank_candidates_with_embedding",
    "_normalize_temperature",
    "_normalize_top_p",
]
