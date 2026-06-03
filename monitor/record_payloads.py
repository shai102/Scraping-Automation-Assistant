import json

from db.scrape_models import ScrapeRecord, SymlinkRecord
from monitor.metadata_refresh import record_to_dict as metadata_record_to_dict


def symlink_record_to_dict(record: SymlinkRecord) -> dict:
    return {
        "id": record.id,
        "folder_id": record.folder_id,
        "original_path": record.original_path,
        "link_path": record.link_path,
        "status": record.status,
        "error_msg": record.error_msg,
        "created_at": record.created_at.isoformat() if record.created_at else None,
    }


def scrape_record_to_dict(record: ScrapeRecord) -> dict:
    return metadata_record_to_dict(record)


def attach_record_metadata_json(record: ScrapeRecord, metadata: dict):
    record.metadata_json = json.dumps(metadata or {}, ensure_ascii=False)
