from core.services.matcher_service import (
    _normalize_temperature,
    get_embedding,
    get_online_embedding,
    ollama_post_json,
    parse_with_ollama,
    pick_candidate_with_ollama,
    pick_candidate_with_openai_compatible,
    rerank_candidates_with_embedding,
)


def parse_with_ollama_runtime(ctx, filename):
    return parse_with_ollama(
        ctx.ollama_url.get().strip(),
        ctx.ollama_model.get().strip(),
        filename,
        ctx._get_ai_temperature(),
        ctx._get_ai_top_p(),
    )


def can_use_ollama_for_pick(ctx):
    return bool(ctx.ollama_url.get().strip() and ctx.ollama_model.get().strip())


def can_use_online_model_for_pick(ctx):
    return bool(
        not ctx.prefer_ollama.get()
        and ctx.sf_api_url.get().strip()
        and ctx.sf_api_key.get().strip()
        and ctx.sf_model.get().strip()
    )


def can_use_embedding_rank(ctx):
    if not ctx.use_embedding_rank.get():
        return False

    source = str(ctx.embedding_source.get() or "local").strip().lower()
    if source == "online":
        return bool(
            ctx.sf_api_url.get().strip()
            and ctx.sf_api_key.get().strip()
            and ctx.online_embedding_model.get().strip()
        )

    return bool(ctx.ollama_url.get().strip() and ctx.embedding_model.get().strip())


def get_runtime_embedding(ctx, text):
    if not can_use_embedding_rank(ctx):
        return None

    source = str(ctx.embedding_source.get() or "local").strip().lower()
    if source == "online":
        return get_online_embedding(
            ctx.sf_api_url.get().strip(),
            ctx.sf_api_key.get().strip(),
            ctx.online_embedding_model.get().strip(),
            text,
            ctx.embedding_cache,
            ctx.cache_lock,
        )

    emb, endpoint = get_embedding(
        ollama_post_json,
        ctx.ollama_url.get().strip(),
        ctx.embedding_model.get().strip(),
        text,
        ctx.embedding_cache,
        ctx.cache_lock,
        ctx.ollama_embed_endpoint,
    )
    ctx.ollama_embed_endpoint = endpoint
    return emb


def rerank_candidates_with_runtime_embedding(ctx, item, query_title, year, is_tv, source_name, candidates):
    if not can_use_embedding_rank(ctx) or not candidates:
        return candidates, None, ""
    return rerank_candidates_with_embedding(
        item,
        query_title,
        year,
        is_tv,
        source_name,
        candidates,
        ctx._get_embedding,
    )


def pick_candidate_with_ollama_runtime(ctx, item, query_title, year, is_tv, source_name, candidates):
    return pick_candidate_with_ollama(
        ollama_post_json,
        _normalize_temperature,
        ctx.ollama_url.get().strip(),
        ctx.ollama_model.get().strip(),
        item,
        query_title,
        year,
        is_tv,
        source_name,
        candidates,
        ctx._get_ai_temperature(),
    )


def pick_candidate_with_online_model_runtime(ctx, item, query_title, year, is_tv, source_name, candidates):
    return pick_candidate_with_openai_compatible(
        ctx.sf_api_url.get().strip(),
        ctx.sf_api_key.get().strip(),
        ctx.sf_model.get().strip(),
        item,
        query_title,
        year,
        is_tv,
        source_name,
        candidates,
        ctx._get_ai_temperature(),
        ctx._get_ai_top_p(),
    )
