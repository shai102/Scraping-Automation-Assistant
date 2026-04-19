import datetime

from sqlalchemy import Column, Integer, String, Boolean, Text, DateTime, ForeignKey
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
