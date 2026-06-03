import logging
from db.scrape_models import MonitorFolder

from monitor.delete_sync_common import delete_per_file_sidecars, dir_real_entries, remove_empty_dirs
from monitor.delete_sync_dir import handle_dir_deleted
from monitor.delete_sync_file import handle_file_deleted

logger = logging.getLogger(__name__)


class DeleteSyncService:
    def __init__(
        self,
        watcher,
        *,
        record_to_dict,
        symlink_record_to_dict,
    ):
        self.watcher = watcher
        self._record_to_dict = record_to_dict
        self._symlink_record_to_dict = symlink_record_to_dict
        self._folder_model = MonitorFolder

    def handle_dir_deleted(self, dir_path: str):
        return handle_dir_deleted(self, dir_path)

    def handle_file_deleted(self, path: str):
        return handle_file_deleted(self, path)
