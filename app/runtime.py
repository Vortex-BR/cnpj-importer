from __future__ import annotations

from dataclasses import dataclass

from app.classifier import CategoryClassifier
from app.config import Settings
from app.db import Database
from app.downloader import ResumableDownloader
from app.importer import ImportService
from app.repository import ImportRepository
from app.scheduler import AutoImportScheduler
from app.source import SourceCatalog
from app.worker import ImportWorker


@dataclass(frozen=True)
class Runtime:
    database: Database
    repository: ImportRepository
    import_service: ImportService
    worker: ImportWorker
    scheduler: AutoImportScheduler


def build_runtime(settings: Settings) -> Runtime:
    database = Database(
        settings.database_url,
        max_size=settings.db_pool_max_size,
    )
    repository = ImportRepository(error_sample_limit=settings.max_error_samples)
    catalog = SourceCatalog(
        settings.casa_dos_dados_base_url,
        timeout=settings.download_timeout,
    )
    downloader = ResumableDownloader(
        settings.cache_dir,
        timeout=settings.download_timeout,
        retries=settings.download_retries,
    )
    classifier = CategoryClassifier.from_yaml(settings.categories_config)
    import_service = ImportService(
        database=database,
        repository=repository,
        catalog=catalog,
        downloader=downloader,
        classifier=classifier,
        batch_size=settings.batch_size,
        max_workers=settings.max_workers,
        run_stale_timeout_seconds=settings.run_stale_timeout_seconds,
    )
    worker = ImportWorker(
        import_service,
        poll_interval=settings.worker_poll_interval_seconds,
    )
    scheduler = AutoImportScheduler(
        import_service,
        enabled=settings.auto_import_enabled,
        check_interval_seconds=settings.auto_import_check_interval_seconds,
        jitter_seconds=settings.auto_import_check_jitter_seconds,
        max_retries=settings.auto_import_max_retries_per_month,
        retry_backoff_seconds=settings.auto_import_retry_backoff_seconds,
    )
    return Runtime(database, repository, import_service, worker, scheduler)
