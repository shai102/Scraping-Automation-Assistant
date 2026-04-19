import os
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase

# When frozen by PyInstaller, place the DB next to the .exe
if getattr(sys, 'frozen', False):
    _BASE_DIR = os.path.dirname(sys.executable)
else:
    _BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DB_PATH = os.path.join(_BASE_DIR, 'media_renamer.db')
DATABASE_URL = f'sqlite:///{DB_PATH}'

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    from db.scrape_models import MonitorFolder, ScrapeRecord  # noqa: F401
    Base.metadata.create_all(bind=engine)

    # Lightweight migration: add columns introduced after initial schema
    import sqlalchemy
    with engine.connect() as conn:
        try:
            conn.execute(sqlalchemy.text(
                "ALTER TABLE monitor_folders ADD COLUMN organize_mode TEXT NOT NULL DEFAULT 'move'"
            ))
            conn.commit()
        except Exception:
            conn.rollback()
        try:
            conn.execute(sqlalchemy.text(
                "ALTER TABLE monitor_folders ADD COLUMN symlink_source TEXT NOT NULL DEFAULT ''"
            ))
            conn.commit()
        except Exception:
            conn.rollback()
