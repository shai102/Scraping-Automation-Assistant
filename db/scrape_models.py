import datetime

from sqlalchemy import Column, Integer, String, Boolean, Text, DateTime, ForeignKey, Index, text
from sqlalchemy.orm import relationship

from db.database import Base


class MonitorFolder(Base):
    __tablename__ = "monitor_folders"

    id = Column(Integer, primary_key=True, autoincrement=True)
    path = Column(String(1024), nullable=False, unique=True)
    target_root = Column(String(1024), nullable=False, default="")
    media_type = Column(String(32), nullable=False, default="auto")  # auto / movie / tv
    data_source = Column(String(32), nullable=False, default="siliconflow_tmdb")
    organize_mode = Column(String(32), nullable=False, default="move")  # move / copy / symlink / hardlink / rename
    symlink_source = Column(String(1024), nullable=False, default="")  # STRM source dir (rename mode only)
    skip_if_scraped = Column(Boolean, nullable=False, default=False)
    preserve_existing_folder = Column(Boolean, nullable=False, default=False)
    enabled = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, default=datetime.datetime.now)

    records = relationship("ScrapeRecord", back_populates="folder", cascade="all, delete-orphan")
    symlink_records = relationship("SymlinkRecord", back_populates="folder", cascade="all, delete-orphan")


class ScrapeRecord(Base):
    __tablename__ = "scrape_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    folder_id = Column(Integer, ForeignKey("monitor_folders.id"), nullable=True)
    original_path = Column(String(2048), nullable=False)
    original_name = Column(String(512), nullable=False)
    status = Column(String(32), nullable=False, default="processing")
    # status: processing | success | pending_manual | failed
    matched_title = Column(String(512), nullable=True)
    matched_id = Column(String(64), nullable=True)
    matched_provider = Column(String(32), nullable=True)
    target_path = Column(Text, nullable=True)
    metadata_json = Column(Text, nullable=True)
    error_msg = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.now)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)

    folder = relationship("MonitorFolder", back_populates="records")


class SymlinkRecord(Base):
    """Records for symlink_export mode — tracks each symlink created."""
    __tablename__ = "symlink_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    folder_id = Column(Integer, ForeignKey("monitor_folders.id"), nullable=True)
    original_path = Column(String(2048), nullable=False)
    link_path = Column(String(2048), nullable=False, default="")
    status = Column(String(32), nullable=False, default="success")  # success | failed
    error_msg = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.now)

    folder = relationship("MonitorFolder", back_populates="symlink_records")


class TaskQueue(Base):
    """Persistent background work queue for scan/retry processing."""
    __tablename__ = "task_queue"
    __table_args__ = (
        Index("idx_task_queue_status_updated", "status", "updated_at"),
        Index("idx_task_queue_folder_id", "folder_id"),
        Index("idx_task_queue_path_key", "path_key"),
        Index(
            "ux_task_queue_active_path_type",
            "path_key",
            "task_type",
            unique=True,
            sqlite_where=text("status IN ('queued', 'running')"),
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    path = Column(String(2048), nullable=False)
    path_key = Column(String(2048), nullable=False)
    folder_id = Column(Integer, nullable=True)
    task_type = Column(String(32), nullable=False, default="scrape")  # scrape | symlink_export
    source = Column(String(32), nullable=False, default="watchdog")
    status = Column(String(32), nullable=False, default="queued")  # queued | running | done | failed | skipped
    attempts = Column(Integer, nullable=False, default=0)
    last_error = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.now)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)


class MetadataRefreshState(Base):
    """Backoff state for automatic metadata patrol refreshes."""
    __tablename__ = "metadata_refresh_state"
    __table_args__ = (
        Index("ux_metadata_refresh_state_record_id", "record_id", unique=True),
        Index("idx_metadata_refresh_state_next_attempt", "next_attempt_at"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    record_id = Column(Integer, nullable=False)
    attempts = Column(Integer, nullable=False, default=0)
    no_progress_count = Column(Integer, nullable=False, default=0)
    last_missing_fields = Column(Text, nullable=True)
    last_error = Column(Text, nullable=True)
    last_attempt_at = Column(DateTime, nullable=True)
    next_attempt_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.now)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)


class FolderScanState(Base):
    """Directory mtime checkpoints used by polling scans."""
    __tablename__ = "folder_scan_state"
    __table_args__ = (
        Index("ux_folder_scan_state_folder_dir", "folder_id", "dir_key", unique=True),
        Index("idx_folder_scan_state_updated", "updated_at"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    folder_id = Column(Integer, nullable=False)
    dir_path = Column(String(2048), nullable=False)
    dir_key = Column(String(2048), nullable=False)
    mtime_ns = Column(Integer, nullable=False, default=0)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
