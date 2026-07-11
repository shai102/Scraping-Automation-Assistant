import os
import sys
from collections.abc import Callable

from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker, DeclarativeBase

# When frozen by PyInstaller, place the DB next to the .exe.
# In Docker mode, DATA_DIR env var redirects persistent files to a mounted volume.
if getattr(sys, 'frozen', False):
    _BASE_DIR = os.path.dirname(sys.executable)
else:
    _BASE_DIR = os.environ.get('DATA_DIR') or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DB_PATH = os.path.join(_BASE_DIR, 'media_renamer.db')
DATABASE_URL = f'sqlite:///{DB_PATH}'

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False, "timeout": 30},
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@event.listens_for(engine, "connect")
def _set_sqlite_pragmas(dbapi_connection, _connection_record):
    if not DATABASE_URL.startswith("sqlite"):
        return
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA busy_timeout=30000")
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
    finally:
        cursor.close()


class Base(DeclarativeBase):
    pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def vacuum_db():
    """Compact SQLite database files after large deletes.

    SQLite keeps deleted rows as reusable free pages, so the .db file size only
    shrinks after VACUUM. VACUUM must run outside an active transaction.
    """
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text("PRAGMA wal_checkpoint(TRUNCATE)"))
        conn.execute(text("VACUUM"))


def _table_columns(conn, table_name: str) -> set[str]:
    rows = conn.exec_driver_sql(f"PRAGMA table_info('{table_name}')").fetchall()
    return {str(row[1]) for row in rows}


def _table_indexes(conn, table_name: str) -> set[str]:
    rows = conn.exec_driver_sql(f"PRAGMA index_list('{table_name}')").fetchall()
    return {str(row[1]) for row in rows}


def _add_column_if_missing(conn, table_name: str, column_name: str, ddl: str):
    if column_name not in _table_columns(conn, table_name):
        conn.execute(text(ddl))


def _create_index_if_missing(conn, table_name: str, index_name: str, ddl: str):
    if index_name not in _table_indexes(conn, table_name):
        conn.execute(text(ddl))


def _ensure_schema_migrations(conn):
    conn.execute(text(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    ))


def _migration_monitor_folder_columns(conn):
    _add_column_if_missing(
        conn,
        "monitor_folders",
        "organize_mode",
        "ALTER TABLE monitor_folders ADD COLUMN organize_mode TEXT NOT NULL DEFAULT 'move'",
    )
    _add_column_if_missing(
        conn,
        "monitor_folders",
        "symlink_source",
        "ALTER TABLE monitor_folders ADD COLUMN symlink_source TEXT NOT NULL DEFAULT ''",
    )
    _add_column_if_missing(
        conn,
        "monitor_folders",
        "skip_if_scraped",
        "ALTER TABLE monitor_folders ADD COLUMN skip_if_scraped BOOLEAN NOT NULL DEFAULT 0",
    )
    _add_column_if_missing(
        conn,
        "monitor_folders",
        "preserve_existing_folder",
        "ALTER TABLE monitor_folders ADD COLUMN preserve_existing_folder BOOLEAN NOT NULL DEFAULT 0",
    )


def _migration_runtime_indexes(conn):
    for table_name, index_name, ddl in (
        (
            "scrape_records",
            "idx_scrape_records_folder_id",
            "CREATE INDEX IF NOT EXISTS idx_scrape_records_folder_id ON scrape_records(folder_id)",
        ),
        (
            "scrape_records",
            "idx_scrape_records_status",
            "CREATE INDEX IF NOT EXISTS idx_scrape_records_status ON scrape_records(status)",
        ),
        (
            "scrape_records",
            "idx_scrape_records_original_path",
            "CREATE INDEX IF NOT EXISTS idx_scrape_records_original_path ON scrape_records(original_path)",
        ),
        (
            "scrape_records",
            "idx_scrape_records_target_path",
            "CREATE INDEX IF NOT EXISTS idx_scrape_records_target_path ON scrape_records(target_path)",
        ),
        (
            "scrape_records",
            "idx_scrape_records_updated_at",
            "CREATE INDEX IF NOT EXISTS idx_scrape_records_updated_at ON scrape_records(updated_at)",
        ),
        (
            "symlink_records",
            "idx_symlink_records_folder_id",
            "CREATE INDEX IF NOT EXISTS idx_symlink_records_folder_id ON symlink_records(folder_id)",
        ),
        (
            "symlink_records",
            "idx_symlink_records_status",
            "CREATE INDEX IF NOT EXISTS idx_symlink_records_status ON symlink_records(status)",
        ),
        (
            "symlink_records",
            "idx_symlink_records_original_path",
            "CREATE INDEX IF NOT EXISTS idx_symlink_records_original_path ON symlink_records(original_path)",
        ),
        (
            "symlink_records",
            "idx_symlink_records_link_path",
            "CREATE INDEX IF NOT EXISTS idx_symlink_records_link_path ON symlink_records(link_path)",
        ),
    ):
        _create_index_if_missing(conn, table_name, index_name, ddl)


def _migration_task_queue(conn):
    conn.execute(text(
        """
        CREATE TABLE IF NOT EXISTS task_queue (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            path VARCHAR(2048) NOT NULL,
            path_key VARCHAR(2048) NOT NULL,
            folder_id INTEGER,
            task_type VARCHAR(32) NOT NULL DEFAULT 'scrape',
            source VARCHAR(32) NOT NULL DEFAULT 'watchdog',
            status VARCHAR(32) NOT NULL DEFAULT 'queued',
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            created_at DATETIME,
            updated_at DATETIME,
            started_at DATETIME,
            finished_at DATETIME
        )
        """
    ))
    for table_name, index_name, ddl in (
        (
            "task_queue",
            "idx_task_queue_status_updated",
            "CREATE INDEX IF NOT EXISTS idx_task_queue_status_updated ON task_queue(status, updated_at)",
        ),
        (
            "task_queue",
            "idx_task_queue_folder_id",
            "CREATE INDEX IF NOT EXISTS idx_task_queue_folder_id ON task_queue(folder_id)",
        ),
        (
            "task_queue",
            "idx_task_queue_path_key",
            "CREATE INDEX IF NOT EXISTS idx_task_queue_path_key ON task_queue(path_key)",
        ),
        (
            "task_queue",
            "ux_task_queue_active_path_type",
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_task_queue_active_path_type "
            "ON task_queue(path_key, task_type) WHERE status IN ('queued', 'running')",
        ),
    ):
        _create_index_if_missing(conn, table_name, index_name, ddl)


def _migration_folder_scan_state(conn):
    conn.execute(text(
        """
        CREATE TABLE IF NOT EXISTS folder_scan_state (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            folder_id INTEGER NOT NULL,
            dir_path VARCHAR(2048) NOT NULL,
            dir_key VARCHAR(2048) NOT NULL,
            mtime_ns INTEGER NOT NULL DEFAULT 0,
            updated_at DATETIME
        )
        """
    ))
    for table_name, index_name, ddl in (
        (
            "folder_scan_state",
            "ux_folder_scan_state_folder_dir",
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_folder_scan_state_folder_dir "
            "ON folder_scan_state(folder_id, dir_key)",
        ),
        (
            "folder_scan_state",
            "idx_folder_scan_state_updated",
            "CREATE INDEX IF NOT EXISTS idx_folder_scan_state_updated ON folder_scan_state(updated_at)",
        ),
    ):
        _create_index_if_missing(conn, table_name, index_name, ddl)


def _migration_metadata_refresh_state(conn):
    conn.execute(text(
        """
        CREATE TABLE IF NOT EXISTS metadata_refresh_state (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            record_id INTEGER NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            no_progress_count INTEGER NOT NULL DEFAULT 0,
            last_missing_fields TEXT,
            last_error TEXT,
            last_attempt_at DATETIME,
            next_attempt_at DATETIME,
            created_at DATETIME,
            updated_at DATETIME
        )
        """
    ))
    for table_name, index_name, ddl in (
        (
            "metadata_refresh_state",
            "ux_metadata_refresh_state_record_id",
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_metadata_refresh_state_record_id "
            "ON metadata_refresh_state(record_id)",
        ),
        (
            "metadata_refresh_state",
            "idx_metadata_refresh_state_next_attempt",
            "CREATE INDEX IF NOT EXISTS idx_metadata_refresh_state_next_attempt "
            "ON metadata_refresh_state(next_attempt_at)",
        ),
    ):
        _create_index_if_missing(conn, table_name, index_name, ddl)


def _migration_archive_operations(conn):
    conn.execute(text(
        """
        CREATE TABLE IF NOT EXISTS archive_operations (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            record_id INTEGER,
            source_path VARCHAR(2048) NOT NULL,
            target_path VARCHAR(2048) NOT NULL,
            organize_mode VARCHAR(32) NOT NULL,
            phase VARCHAR(32) NOT NULL DEFAULT 'prepared',
            status VARCHAR(32) NOT NULL DEFAULT 'running',
            error_msg TEXT,
            created_at DATETIME,
            updated_at DATETIME,
            completed_at DATETIME
        )
        """
    ))
    for table_name, index_name, ddl in (
        (
            "archive_operations",
            "idx_archive_operations_status_updated",
            "CREATE INDEX IF NOT EXISTS idx_archive_operations_status_updated "
            "ON archive_operations(status, updated_at)",
        ),
        (
            "archive_operations",
            "idx_archive_operations_record_id",
            "CREATE INDEX IF NOT EXISTS idx_archive_operations_record_id "
            "ON archive_operations(record_id)",
        ),
    ):
        _create_index_if_missing(conn, table_name, index_name, ddl)


_MIGRATIONS: tuple[tuple[str, Callable], ...] = (
    ("0001_monitor_folder_columns", _migration_monitor_folder_columns),
    ("0002_runtime_indexes", _migration_runtime_indexes),
    ("0003_task_queue", _migration_task_queue),
    ("0004_folder_scan_state", _migration_folder_scan_state),
    ("0005_metadata_refresh_state", _migration_metadata_refresh_state),
    ("0006_archive_operations", _migration_archive_operations),
)


def _run_migrations():
    with engine.begin() as conn:
        _ensure_schema_migrations(conn)
        applied = {
            row[0]
            for row in conn.execute(text("SELECT version FROM schema_migrations")).fetchall()
        }
        for version, migration in _MIGRATIONS:
            if version in applied:
                continue
            migration(conn)
            conn.execute(
                text("INSERT OR IGNORE INTO schema_migrations(version) VALUES (:version)"),
                {"version": version},
            )


def init_db():
    from db.scrape_models import (  # noqa: F401
        ArchiveOperation,
        FolderScanState,
        MonitorFolder,
        MetadataRefreshState,
        ScrapeRecord,
        SymlinkRecord,
        TaskQueue,
    )
    Base.metadata.create_all(bind=engine)
    _run_migrations()
