"""Single commit-and-broadcast boundary for scrape record state changes."""

from monitor.record_payloads import scrape_record_to_dict


def persist_record_update(db, record, broadcast_fn=None, *, commit: bool = True):
    if commit:
        db.commit()
    else:
        db.flush()
    payload = scrape_record_to_dict(record)
    if callable(broadcast_fn):
        broadcast_fn({"type": "record_update", "data": payload})
    return payload
