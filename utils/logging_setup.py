import logging

from utils.logging_filters import (
    ErrorLogFilter,
    GeneralLogFilter,
    MetadataLogFilter,
    ScrapeLogFilter,
)
from utils.logging_handlers import DatePartitionedFileHandler
from utils.logging_paths import (
    LOG_KIND_APP,
    LOG_KIND_METADATA,
    LOG_KIND_SCRAPE,
    list_available_log_dates,
    normalize_log_kind,
    resolve_log_dir,
    resolve_log_path,
)


def setup_logging(data_dir: str, console_stream=None) -> None:
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    app_handler = DatePartitionedFileHandler(data_dir, LOG_KIND_APP)
    app_handler.setLevel(logging.INFO)
    app_handler.addFilter(GeneralLogFilter())
    app_handler.setFormatter(fmt)
    root_logger.addHandler(app_handler)

    scrape_handler = DatePartitionedFileHandler(data_dir, LOG_KIND_SCRAPE)
    scrape_handler.setLevel(logging.INFO)
    scrape_handler.addFilter(ScrapeLogFilter())
    scrape_handler.setFormatter(fmt)
    root_logger.addHandler(scrape_handler)

    metadata_handler = DatePartitionedFileHandler(data_dir, LOG_KIND_METADATA)
    metadata_handler.setLevel(logging.INFO)
    metadata_handler.addFilter(MetadataLogFilter())
    metadata_handler.setFormatter(fmt)
    root_logger.addHandler(metadata_handler)

    if console_stream is not None:
        console_handler = logging.StreamHandler(console_stream)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(fmt)
        root_logger.addHandler(console_handler)
