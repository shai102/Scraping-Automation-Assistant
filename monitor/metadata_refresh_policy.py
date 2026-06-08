from core.metadata.completeness import metadata_missing_fields


def metadata_refresh_options(worker_ctx) -> dict:
    cfg = getattr(worker_ctx, "_cfg", {}) if worker_ctx else {}
    return {
        "ignore_episode_title_rules": cfg.get("metadata_refresh_ignore_episode_title_rules", []),
        "skip_rules": cfg.get("metadata_refresh_skip_rules", []),
    }


def missing_fields_for_record(record, options: dict) -> list[str]:
    return metadata_missing_fields(
        record.metadata_json or "",
        ignore_episode_title_rules=options.get("ignore_episode_title_rules"),
        skip_rules=options.get("skip_rules"),
        title_hint=record.matched_title or record.original_name or "",
        matched_id=record.matched_id or "",
        provider_hint=record.matched_provider or "",
    )
