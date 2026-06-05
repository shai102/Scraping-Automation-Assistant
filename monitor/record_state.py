import os

from db.scrape_models import ScrapeRecord


def has_nfo(filepath: str) -> bool:
    base = os.path.splitext(filepath)[0]
    return os.path.isfile(base + ".nfo")


def is_already_scraped(filepath: str, sub_audio_exts: tuple) -> bool:
    if sub_audio_exts and filepath.lower().endswith(sub_audio_exts):
        parent = os.path.dirname(filepath)
        if os.path.isfile(os.path.join(parent, "season.nfo")):
            return True
        skip_names = {"season.nfo", "tvshow.nfo", "folder.nfo"}
        try:
            for filename in os.listdir(parent):
                if filename.lower().endswith(".nfo") and filename.lower() not in skip_names:
                    return True
        except OSError:
            pass
        return False
    return has_nfo(filepath)


def symlink_record_needs_repair(row) -> bool:
    if not row or str(getattr(row, "status", "") or "").lower() != "success":
        return False
    original_path = str(getattr(row, "original_path", "") or "").strip()
    link_path = str(getattr(row, "link_path", "") or "").strip()
    if not original_path or not os.path.isfile(original_path):
        return False
    if not link_path:
        return True
    return not os.path.lexists(link_path)


def scrape_record_needs_repair(row, ctx) -> bool:
    if not row or str(getattr(row, "status", "") or "").lower() != "success":
        return False

    target_path = str(getattr(row, "target_path", "") or "").strip()
    original_path = str(getattr(row, "original_path", "") or "").strip()
    sub_audio_exts = ctx.get_sub_audio_exts() if ctx else ()

    if target_path and os.path.isfile(target_path):
        return not is_already_scraped(target_path, sub_audio_exts)
    if original_path and os.path.isfile(original_path):
        return True
    return False


def symlink_record_consumed_downstream(row, db, ctx) -> bool:
    if db is None:
        return False
    link_path = str(getattr(row, "link_path", "") or "").strip()
    if not link_path:
        return False

    downstream = (
        db.query(ScrapeRecord)
        .filter(
            ScrapeRecord.original_path == link_path,
            ScrapeRecord.status == "success",
        )
        .order_by(ScrapeRecord.id.desc())
        .first()
    )
    if not downstream:
        return False
    return not scrape_record_needs_repair(downstream, ctx)


def symlink_source_consumed_downstream(folder, source_path: str, db, ctx) -> bool:
    if db is None or folder is None:
        return False

    source_root = str(getattr(folder, "path", "") or "").strip()
    target_root = str(getattr(folder, "target_root", "") or "").strip()
    if not source_root or not target_root:
        return False

    try:
        rel_path = os.path.relpath(os.path.normpath(source_path), os.path.normpath(source_root))
    except ValueError:
        return False

    if rel_path.startswith(".."):
        return False

    expected_link_path = os.path.normpath(os.path.join(target_root, rel_path))
    downstream = (
        db.query(ScrapeRecord)
        .filter(
            ScrapeRecord.original_path == expected_link_path,
            ScrapeRecord.status == "success",
        )
        .order_by(ScrapeRecord.id.desc())
        .first()
    )
    if not downstream:
        return False
    return not scrape_record_needs_repair(downstream, ctx)


def reset_scrape_record_for_rebuild(record):
    record._repairing = True
    record._previous_target = str(getattr(record, "target_path", "") or "")
    record.status = "processing"
    record.matched_title = None
    record.matched_id = None
    record.matched_provider = None
    record.metadata_json = None
    record.error_msg = None
