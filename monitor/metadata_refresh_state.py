import datetime

from db.scrape_models import MetadataRefreshState


NO_PROGRESS_BACKOFF_HOURS = (12, 24, 48, 96, 168, 336)


def get_or_create_state(db, record_id: int, now: datetime.datetime):
    state = (
        db.query(MetadataRefreshState)
        .filter(MetadataRefreshState.record_id == record_id)
        .first()
    )
    if state:
        return state
    state = MetadataRefreshState(
        record_id=record_id,
        attempts=0,
        no_progress_count=0,
        created_at=now,
        updated_at=now,
    )
    db.add(state)
    db.flush()
    return state


def should_skip_for_backoff(db, record_id: int, now: datetime.datetime) -> bool:
    state = (
        db.query(MetadataRefreshState)
        .filter(MetadataRefreshState.record_id == record_id)
        .first()
    )
    return bool(state and state.next_attempt_at and state.next_attempt_at > now)


def mark_refresh_result(
    db,
    record_id: int,
    *,
    before_missing: list[str],
    after_missing: list[str],
    updated: bool,
    error: str | None,
    now: datetime.datetime,
    get_state_fn=None,
) -> None:
    state_loader = get_state_fn or get_or_create_state
    state = state_loader(db, record_id, now)
    state.attempts = int(state.attempts or 0) + 1
    state.last_attempt_at = now
    state.last_missing_fields = ",".join(after_missing or before_missing)
    state.last_error = (error or "")[:1000] if error else None
    state.updated_at = now

    made_progress = bool(error is None and (updated or len(after_missing) < len(before_missing)))
    if made_progress:
        state.no_progress_count = 0
        state.next_attempt_at = None
    else:
        state.no_progress_count = int(state.no_progress_count or 0) + 1
        index = min(state.no_progress_count - 1, len(NO_PROGRESS_BACKOFF_HOURS) - 1)
        state.next_attempt_at = now + datetime.timedelta(hours=NO_PROGRESS_BACKOFF_HOURS[index])
    db.commit()
