import json

from db.scrape_models import ScrapeRecord
from utils.value_utils import normalize_parse_source


def record_to_dict(record: ScrapeRecord) -> dict:
    parse_source = None
    confidence = None
    confidence_level = None
    if record.metadata_json:
        try:
            metadata = json.loads(record.metadata_json)
            parse_source = normalize_parse_source(metadata.get("parse_source"))
            confidence = metadata.get("confidence")
            confidence_level = metadata.get("confidence_level")
        except Exception:
            pass
    return {
        "id": record.id,
        "folder_id": record.folder_id,
        "original_path": record.original_path,
        "original_name": record.original_name,
        "status": record.status,
        "matched_title": record.matched_title,
        "matched_id": record.matched_id,
        "matched_provider": record.matched_provider,
        "target_path": record.target_path,
        "parse_source": parse_source,
        "confidence": confidence,
        "confidence_level": confidence_level,
        "error_msg": record.error_msg,
        "created_at": record.created_at.isoformat() if record.created_at else None,
        "updated_at": record.updated_at.isoformat() if record.updated_at else None,
    }
