from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator, Sequence

from app.classifier import CategoryClassifier
from app.downloader import DownloadResult, ResumableDownloader
from app.models import (
    AutoImportCheck,
    CompanyRecord,
    EstablishmentRecord,
    PartnerRecord,
    SourceFile,
)
from app.normalization import normalize_nature_group
from app.parser import (
    parse_company_row,
    parse_establishment_row,
    parse_partner_row,
    stream_zip_rows,
)
from app.repository import ActiveRunConflict, ImportRepository
from app.source import NoCompleteSnapshotError, SourceCatalog, SourceManifest
from app.worker import RunHeartbeat


LOGGER = logging.getLogger(__name__)
ADVISORY_LOCK_ID = 748392115
ENQUEUE_LOCK_ID = 748392116
AUTO_SCHEDULER_LOCK_ID = 748392117


@dataclass(frozen=True)
class RowError:
    cnpj: str | None
    message: str


@dataclass(frozen=True)
class EstablishmentBatch:
    processed_rows: int
    total_rows: int
    records: tuple[EstablishmentRecord, ...]
    errors: tuple[RowError, ...]

    @property
    def error_rows(self) -> int:
        return len(self.errors)


@dataclass(frozen=True)
class PartnerBatch:
    processed_rows: int
    total_rows: int
    records: tuple[PartnerRecord, ...]
    errors: tuple[RowError, ...]

    @property
    def error_rows(self) -> int:
        return len(self.errors)


def iter_establishment_batches(
    path: str | Path,
    *,
    batch_size: int,
    skip_rows: int = 0,
) -> Iterator[EstablishmentBatch]:
    records: list[EstablishmentRecord] = []
    errors: list[RowError] = []
    chunk_rows = 0
    processed_rows = skip_rows
    for row_number, row in enumerate(stream_zip_rows(path), start=1):
        if row_number <= skip_rows:
            continue
        processed_rows = row_number
        chunk_rows += 1
        try:
            record = parse_establishment_row(row)
            if record is not None:
                records.append(record)
        except (ValueError, IndexError) as exc:
            cnpj = None
            if len(row) >= 3:
                candidate = "".join(part.strip() for part in row[:3])
                cnpj = candidate if len(candidate) == 14 and candidate.isdigit() else None
            errors.append(RowError(cnpj, str(exc)))
        if chunk_rows >= batch_size:
            yield EstablishmentBatch(
                processed_rows,
                chunk_rows,
                tuple(records),
                tuple(errors),
            )
            records.clear()
            errors.clear()
            chunk_rows = 0
    if chunk_rows:
        yield EstablishmentBatch(
            processed_rows,
            chunk_rows,
            tuple(records),
            tuple(errors),
        )


def iter_partner_batches(
    path: str | Path,
    *,
    batch_size: int,
    skip_rows: int = 0,
) -> Iterator[PartnerBatch]:
    records: list[PartnerRecord] = []
    errors: list[RowError] = []
    chunk_rows = 0
    processed_rows = skip_rows
    for row_number, row in enumerate(stream_zip_rows(path), start=1):
        if row_number <= skip_rows:
            continue
        processed_rows = row_number
        chunk_rows += 1
        try:
            records.append(parse_partner_row(row))
        except (ValueError, IndexError) as exc:
            errors.append(RowError(None, str(exc)))
        if chunk_rows >= batch_size:
            yield PartnerBatch(
                processed_rows,
                chunk_rows,
                tuple(records),
                tuple(errors),
            )
            records.clear()
            errors.clear()
            chunk_rows = 0
    if chunk_rows:
        yield PartnerBatch(
            processed_rows,
            chunk_rows,
            tuple(records),
            tuple(errors),
        )


class ImportService:
    def __init__(
        self,
        *,
        database,
        repository: ImportRepository,
        catalog: SourceCatalog,
        downloader: ResumableDownloader,
        classifier: CategoryClassifier,
        batch_size: int,
        max_workers: int,
        run_stale_timeout_seconds: int = 21600,
        clock=datetime.utcnow,
        source_name: str = "casa_dos_dados",
    ) -> None:
        self.database = database
        self.repository = repository
        self.catalog = catalog
        self.downloader = downloader
        self.classifier = classifier
        self.batch_size = batch_size
        self.max_workers = max(1, max_workers)
        self.run_stale_timeout_seconds = run_stale_timeout_seconds
        self.clock = clock
        self.source_name = source_name

    def enqueue_latest(self) -> int:
        self._ensure_no_active_run()
        return self._enqueue(self.catalog.resolve_latest(), trigger_type="MANUAL")

    def enqueue_month(self, source_month: str) -> int:
        self._ensure_no_active_run()
        return self._enqueue(
            self.catalog.resolve_month(source_month),
            trigger_type="MANUAL",
        )

    def _ensure_no_active_run(self) -> None:
        with self.database.connection() as connection:
            self.repository.recover_stale_runs(
                connection,
                timeout_seconds=self.run_stale_timeout_seconds,
            )
            self.repository.recover_orphaned_runs(
                connection,
                lock_id=ADVISORY_LOCK_ID,
            )
            active_run = self.repository.get_active_run(connection)
        if active_run:
            raise ActiveRunConflict(active_run)

    def _enqueue(self, manifest: SourceManifest, *, trigger_type: str) -> int:
        with self.database.connection() as connection:
            run_id = self.repository.create_run_exclusive(
                connection,
                source_month=manifest.source_month,
                source_url=manifest.directory_url,
                trigger_type=trigger_type,
                stale_timeout_seconds=self.run_stale_timeout_seconds,
                enqueue_lock_id=ENQUEUE_LOCK_ID,
                import_lock_id=ADVISORY_LOCK_ID,
                source_name=self.source_name,
            )
            self.repository.initialize_files(
                connection,
                run_id=run_id,
                files=manifest.files,
            )
        return run_id

    def list_source_months(self) -> list[dict]:
        months = self.catalog.list_months()
        with self.database.connection() as connection:
            statuses = self.repository.get_month_statuses(
                connection,
                [month.source_month for month in months],
            )
        return [
            {
                "source_month": month.source_month,
                "last_modified": month.last_modified,
                "is_complete": month.is_complete,
                "already_imported": statuses[month.source_month]["already_imported"],
                "status": statuses[month.source_month]["status"],
            }
            for month in months
        ]

    def check_auto_import_once(
        self,
        *,
        max_retries: int,
        retry_backoff_seconds: int,
    ) -> AutoImportCheck:
        with self.database.connection() as connection:
            if not self.repository.try_advisory_lock(
                connection,
                lock_id=AUTO_SCHEDULER_LOCK_ID,
            ):
                return AutoImportCheck("ACTIVE_RUN_EXISTS", None, None)
            try:
                self.repository.recover_stale_runs(
                    connection,
                    timeout_seconds=self.run_stale_timeout_seconds,
                )
                self.repository.recover_orphaned_runs(
                    connection,
                    lock_id=ADVISORY_LOCK_ID,
                )
                active_run = self.repository.get_active_run(connection)
                if active_run:
                    return AutoImportCheck(
                        "ACTIVE_RUN_EXISTS",
                        active_run["source_month"],
                        active_run["id"],
                    )
                try:
                    manifest = self.catalog.resolve_latest()
                except NoCompleteSnapshotError:
                    return AutoImportCheck("NO_COMPLETE_SNAPSHOT", None, None)
                retry_state = self.repository.get_auto_retry_state(
                    connection,
                    source_month=manifest.source_month,
                )
                if retry_state.already_imported:
                    return AutoImportCheck(
                        "NO_NEW_SNAPSHOT",
                        manifest.source_month,
                        None,
                    )
                if (
                    retry_state.failed_auto_attempts > 0
                    and retry_state.failed_auto_attempts >= max_retries
                ):
                    return AutoImportCheck(
                        "RETRY_LIMIT_REACHED",
                        manifest.source_month,
                        None,
                    )
                if (
                    retry_state.last_auto_failure_at is not None
                    and retry_state.last_auto_failure_at
                    + timedelta(seconds=retry_backoff_seconds)
                    > self.clock()
                ):
                    return AutoImportCheck(
                        "RETRY_BACKOFF",
                        manifest.source_month,
                        None,
                    )
                try:
                    run_id = self.repository.create_run_exclusive(
                        connection,
                        source_month=manifest.source_month,
                        source_url=manifest.directory_url,
                        trigger_type="AUTO",
                        stale_timeout_seconds=self.run_stale_timeout_seconds,
                        enqueue_lock_id=ENQUEUE_LOCK_ID,
                        import_lock_id=ADVISORY_LOCK_ID,
                        source_name=self.source_name,
                    )
                except ActiveRunConflict as exc:
                    return AutoImportCheck(
                        "ACTIVE_RUN_EXISTS",
                        exc.active_run["source_month"],
                        exc.active_run["id"],
                    )
                self.repository.initialize_files(
                    connection,
                    run_id=run_id,
                    files=manifest.files,
                )
                return AutoImportCheck(
                    "ENQUEUED",
                    manifest.source_month,
                    run_id,
                )
            finally:
                self.repository.release_advisory_lock(
                    connection,
                    lock_id=AUTO_SCHEDULER_LOCK_ID,
                )

    def update_auto_import_state(
        self,
        *,
        checked_at,
        result,
        next_check_at,
        source_month,
    ) -> None:
        with self.database.connection() as connection:
            self.repository.update_auto_import_state(
                connection,
                checked_at=checked_at,
                result=result,
                next_check_at=next_check_at,
                source_month=source_month,
            )

    def get_auto_import_state(self):
        with self.database.connection() as connection:
            return self.repository.get_auto_import_state(connection)

    def process_queued_once(self) -> bool:
        with self.database.connection() as connection:
            run = self.repository.claim_next_run(connection)
        if not run:
            return False
        run_id = int(run["id"] if isinstance(run, dict) else run[0])
        return self.run_import(run_id)

    def run_import(self, run_id: int) -> bool:
        with self.database.connection() as connection:
            self.repository.start_run(connection, run_id=run_id)
            run = self.repository.get_run(connection, run_id)
            if not run:
                raise ValueError(f"importação {run_id} não encontrada")
            source_month = run["source_month"]
            locked = connection.execute(
                "SELECT pg_try_advisory_lock(%s) AS locked",
                (ADVISORY_LOCK_ID,),
            ).fetchone()
            lock_acquired = bool(
                locked["locked"] if isinstance(locked, dict) else locked[0]
            )
            connection.commit()
            if not lock_acquired:
                self.repository.requeue_run(connection, run_id=run_id)
                return False
            heartbeat = None
            try:
                manifest = self.catalog.resolve_month(source_month)
                with connection.transaction():
                    self.repository.initialize_files(
                        connection,
                        run_id=run_id,
                        files=manifest.files,
                    )
                heartbeat = RunHeartbeat(
                    self.database,
                    self.repository,
                    run_id=run_id,
                    interval_seconds=max(
                        5,
                        min(60, self.run_stale_timeout_seconds / 3),
                    ),
                )
                heartbeat.start()
                files_by_name = {
                    source_file.name: source_file
                    for source_file in manifest.files
                }
                self._prepare_staging(
                    connection,
                    run_id=run_id,
                    manifest=manifest,
                )
                self._process_auxiliaries(
                    connection,
                    run_id=run_id,
                    source_month=source_month,
                    manifest=manifest,
                    files_by_name=files_by_name,
                )
                self._process_companies(
                    connection,
                    run_id=run_id,
                    source_month=source_month,
                    manifest=manifest,
                    files_by_name=files_by_name,
                )
                self._process_establishments(
                    connection,
                    run_id=run_id,
                    source_month=source_month,
                    manifest=manifest,
                    files_by_name=files_by_name,
                )
                with connection.transaction():
                    self.repository.truncate_company_staging(connection)
                with connection.transaction():
                    self._build_active_roots_table(connection)
                self._load_partners(
                    connection,
                    run_id=run_id,
                    source_month=source_month,
                    manifest=manifest,
                    files_by_name=files_by_name,
                )
                with connection.transaction():
                    connection.execute("DROP TABLE IF EXISTS active_cnpj_roots")
                with connection.transaction():
                    self.repository.touch_run(
                        connection,
                        run_id=run_id,
                        phase="RECONCILING",
                    )
                self._finalize_snapshot(
                    connection,
                    run_id=run_id,
                    source_month=source_month,
                )
                return True
            except Exception as exc:
                LOGGER.exception("Importação %s falhou", run_id)
                connection.rollback()
                with connection.transaction():
                    self.repository.fail_run(
                        connection,
                        run_id=run_id,
                        error_message=str(exc),
                    )
                raise
            finally:
                if heartbeat is not None:
                    heartbeat.stop()
                connection.execute(
                    "SELECT pg_advisory_unlock(%s)",
                    (ADVISORY_LOCK_ID,),
                )
                connection.commit()

    def _prepare_staging(
        self,
        connection,
        *,
        run_id: int,
        manifest: SourceManifest,
    ) -> None:
        rebuildable_files = {
            "Cnaes.zip",
            "Municipios.zip",
            "Naturezas.zip",
            "Simples.zip",
            "Qualificacoes.zip",
            *(
                f"Empresas{index}.zip"
                for index in range(10)
            ),
        }
        with connection.transaction():
            checkpoints = {
                source_file.name: self.repository.get_file(
                    connection,
                    run_id=run_id,
                    file_name=source_file.name,
                )
                for source_file in manifest.files
            }
            partner_progress = any(
                checkpoint["status"] != "PENDING"
                or int(checkpoint.get("processed_rows") or 0) > 0
                for file_name, checkpoint in checkpoints.items()
                if file_name.startswith("Socios")
            )
            self.repository.touch_run(
                connection,
                run_id=run_id,
                phase="STAGING",
            )
            if not partner_progress:
                self.repository.reset_staging(connection)
                for file_name in rebuildable_files:
                    checkpoint = checkpoints.get(file_name)
                    if checkpoint and checkpoint["status"] != "PENDING":
                        self.repository.reset_file_checkpoint(
                            connection,
                            run_id=run_id,
                            file_name=file_name,
                        )
        if partner_progress:
            LOGGER.info(
                "Run %s retomada; staging existente preservado",
                run_id,
            )

    def _get_file_checkpoint(self, connection, *, run_id: int, file_name: str):
        with connection.transaction():
            return self.repository.get_file(
                connection,
                run_id=run_id,
                file_name=file_name,
            )

    def _download_one(
        self,
        source_month: str,
        source_file: SourceFile,
        *,
        connection,
        run_id: int,
    ) -> DownloadResult:
        with connection.transaction():
            self.repository.touch_run(
                connection,
                run_id=run_id,
                phase=f"DOWNLOADING:{source_file.name}",
                file_name=source_file.name,
            )
        result = self.downloader.download(source_month, source_file)
        with connection.transaction():
            self.repository.mark_downloaded(
                connection,
                run_id=run_id,
                file_name=source_file.name,
                etag=result.etag,
                content_length=result.content_length,
            )
        LOGGER.info("Download concluído: %s", source_file.name)
        return result

    def _delete_zip(self, path: Path) -> None:
        try:
            path.unlink(missing_ok=True)
            meta = path.with_suffix(path.suffix + ".meta.json")
            meta.unlink(missing_ok=True)
            LOGGER.info("ZIP removido: %s", path.name)
        except OSError as exc:
            LOGGER.warning("Falha ao remover ZIP %s: %s", path.name, exc)

    def _process_auxiliaries(
        self,
        connection,
        *,
        run_id: int,
        source_month: str,
        manifest: SourceManifest,
        files_by_name: dict[str, SourceFile],
    ) -> None:
        del manifest
        auxiliaries = (
            ("Cnaes.zip", "stg_cnaes"),
            ("Municipios.zip", "stg_municipios"),
            ("Naturezas.zip", "stg_naturezas"),
            ("Simples.zip", None),
            ("Qualificacoes.zip", "stg_qualificacoes"),
        )
        for file_name, table_name in auxiliaries:
            checkpoint = self._get_file_checkpoint(
                connection,
                run_id=run_id,
                file_name=file_name,
            )
            if checkpoint["status"] == "PROCESSED":
                LOGGER.info("%s já processado, pulando", file_name)
                continue
            result = self._download_one(
                source_month,
                files_by_name[file_name],
                connection=connection,
                run_id=run_id,
            )
            if table_name is None:
                self._load_mei(
                    connection,
                    run_id,
                    source_month,
                    result.path,
                )
            else:
                self._load_reference(
                    connection,
                    run_id,
                    source_month,
                    file_name,
                    result.path,
                    table_name,
                )
            self._delete_zip(result.path)

    def _process_companies(
        self,
        connection,
        *,
        run_id: int,
        source_month: str,
        manifest: SourceManifest,
        files_by_name: dict[str, SourceFile],
    ) -> None:
        del manifest
        for index in range(10):
            file_name = f"Empresas{index}.zip"
            checkpoint = self._get_file_checkpoint(
                connection,
                run_id=run_id,
                file_name=file_name,
            )
            if checkpoint["status"] == "PROCESSED":
                LOGGER.info("%s já processado, pulando", file_name)
                continue
            result = self._download_one(
                source_month,
                files_by_name[file_name],
                connection=connection,
                run_id=run_id,
            )
            self._load_companies(
                connection,
                run_id,
                source_month,
                file_name,
                result.path,
            )
            self._delete_zip(result.path)

    def _load_reference(
        self,
        connection,
        run_id: int,
        source_month: str,
        file_name: str,
        path: Path,
        table_name: str,
    ) -> None:
        with connection.transaction():
            self.repository.reset_file_checkpoint(
                connection, run_id=run_id, file_name=file_name
            )
        batch: list[tuple] = []
        errors: list[RowError] = []
        processed = 0
        for processed, row in enumerate(stream_zip_rows(path), start=1):
            try:
                if len(row) < 2 or not row[0].strip() or not row[1].strip():
                    raise ValueError("linha de referência inválida")
                code, description = row[0].strip(), row[1].strip()
                if table_name == "stg_naturezas":
                    value = (code, description, normalize_nature_group(description))
                elif table_name == "stg_cnaes":
                    value = (
                        code,
                        description,
                        self.classifier.classify(code, description),
                    )
                else:
                    value = (code, description)
                batch.append(value)
            except (ValueError, IndexError) as exc:
                errors.append(RowError(None, str(exc)))
            if len(batch) + len(errors) >= self.batch_size:
                self._flush_reference_batch(
                    connection,
                    run_id,
                    source_month,
                    file_name,
                    table_name,
                    processed,
                    batch,
                    errors,
                )
                batch, errors = [], []
        if batch or errors:
            self._flush_reference_batch(
                connection,
                run_id,
                source_month,
                file_name,
                table_name,
                processed,
                batch,
                errors,
            )
        with connection.transaction():
            self.repository.mark_file_processed(
                connection, run_id=run_id, file_name=file_name
            )

    def _flush_reference_batch(
        self,
        connection,
        run_id: int,
        source_month: str,
        file_name: str,
        table_name: str,
        processed: int,
        rows: Sequence[tuple],
        errors: Sequence[RowError],
    ) -> None:
        columns = {
            "stg_naturezas": ("codigo", "descricao", "natureza_grupo"),
            "stg_municipios": ("codigo", "descricao"),
            "stg_cnaes": ("codigo", "descricao", "categoria_macro"),
            "stg_qualificacoes": ("codigo", "descricao"),
        }[table_name]
        with connection.transaction():
            if rows:
                self.repository.copy_reference_rows(
                    connection,
                    table_name=table_name,
                    columns=columns,
                    rows=rows,
                )
            self._log_errors(
                connection, run_id, source_month, file_name, errors
            )
            self.repository.advance_file_checkpoint(
                connection,
                run_id=run_id,
                file_name=file_name,
                processed_rows=processed,
                total_delta=len(rows) + len(errors),
                active_delta=0,
                inserted_delta=0,
                updated_delta=0,
                error_delta=len(errors),
            )

    def _load_mei(
        self,
        connection,
        run_id: int,
        source_month: str,
        path: Path,
    ) -> None:
        file_name = "Simples.zip"
        with connection.transaction():
            self.repository.reset_file_checkpoint(
                connection, run_id=run_id, file_name=file_name
            )
        batch: list[tuple[str]] = []
        errors: list[RowError] = []
        processed = 0
        chunk_rows = 0
        for processed, row in enumerate(stream_zip_rows(path), start=1):
            chunk_rows += 1
            try:
                if len(row) < 5:
                    raise ValueError("linha do Simples com menos de 5 colunas")
                cnpj_basico = row[0].strip()
                if len(cnpj_basico) != 8 or not cnpj_basico.isdigit():
                    raise ValueError("cnpj_basico inválido no Simples")
                if row[4].strip().upper() == "S":
                    batch.append((cnpj_basico,))
            except (ValueError, IndexError) as exc:
                errors.append(RowError(None, str(exc)))
            if chunk_rows >= self.batch_size:
                self._flush_simple_batch(
                    connection,
                    run_id,
                    source_month,
                    file_name,
                    processed,
                    chunk_rows,
                    batch,
                    errors,
                )
                batch, errors, chunk_rows = [], [], 0
        if chunk_rows:
            self._flush_simple_batch(
                connection,
                run_id,
                source_month,
                file_name,
                processed,
                chunk_rows,
                batch,
                errors,
            )
        with connection.transaction():
            self.repository.mark_file_processed(
                connection, run_id=run_id, file_name=file_name
            )

    def _flush_simple_batch(
        self,
        connection,
        run_id: int,
        source_month: str,
        file_name: str,
        processed: int,
        chunk_rows: int,
        rows: Sequence[tuple[str]],
        errors: Sequence[RowError],
    ) -> None:
        with connection.transaction():
            if rows:
                self.repository.copy_reference_rows(
                    connection,
                    table_name="stg_mei",
                    columns=("cnpj_basico",),
                    rows=rows,
                )
            self._log_errors(
                connection, run_id, source_month, file_name, errors
            )
            self.repository.advance_file_checkpoint(
                connection,
                run_id=run_id,
                file_name=file_name,
                processed_rows=processed,
                total_delta=chunk_rows,
                active_delta=0,
                inserted_delta=0,
                updated_delta=0,
                error_delta=len(errors),
            )

    def _load_companies(
        self,
        connection,
        run_id: int,
        source_month: str,
        file_name: str,
        path: Path,
    ) -> None:
        with connection.transaction():
            self.repository.reset_file_checkpoint(
                connection, run_id=run_id, file_name=file_name
            )
        records: list[CompanyRecord] = []
        errors: list[RowError] = []
        processed = 0
        chunk_rows = 0
        for processed, row in enumerate(stream_zip_rows(path), start=1):
            chunk_rows += 1
            try:
                records.append(parse_company_row(row))
            except (ValueError, IndexError) as exc:
                errors.append(RowError(None, str(exc)))
            if chunk_rows >= self.batch_size:
                self._flush_company_batch(
                    connection,
                    run_id,
                    source_month,
                    file_name,
                    processed,
                    chunk_rows,
                    records,
                    errors,
                )
                records, errors, chunk_rows = [], [], 0
        if chunk_rows:
            self._flush_company_batch(
                connection,
                run_id,
                source_month,
                file_name,
                processed,
                chunk_rows,
                records,
                errors,
            )
        with connection.transaction():
            self.repository.mark_file_processed(
                connection, run_id=run_id, file_name=file_name
            )

    def _flush_company_batch(
        self,
        connection,
        run_id: int,
        source_month: str,
        file_name: str,
        processed: int,
        chunk_rows: int,
        records: Sequence[CompanyRecord],
        errors: Sequence[RowError],
    ) -> None:
        with connection.transaction():
            if records:
                self.repository.copy_companies(connection, records)
            self._log_errors(
                connection, run_id, source_month, file_name, errors
            )
            self.repository.advance_file_checkpoint(
                connection,
                run_id=run_id,
                file_name=file_name,
                processed_rows=processed,
                total_delta=chunk_rows,
                active_delta=0,
                inserted_delta=0,
                updated_delta=0,
                error_delta=len(errors),
            )

    def _process_establishments(
        self,
        connection,
        *,
        run_id: int,
        source_month: str,
        manifest: SourceManifest,
        files_by_name: dict[str, SourceFile],
    ) -> None:
        del manifest
        for index in range(10):
            file_name = f"Estabelecimentos{index}.zip"
            checkpoint = self._get_file_checkpoint(
                connection, run_id=run_id, file_name=file_name
            )
            if checkpoint["status"] == "PROCESSED":
                LOGGER.info("%s já processado, pulando", file_name)
                continue
            skip_rows = int(checkpoint.get("processed_rows") or 0)
            result = self._download_one(
                source_month,
                files_by_name[file_name],
                connection=connection,
                run_id=run_id,
            )
            for batch in iter_establishment_batches(
                result.path,
                batch_size=self.batch_size,
                skip_rows=skip_rows,
            ):
                with connection.transaction():
                    self.repository.copy_establishments(
                        connection, batch.records
                    )
                    candidates, inserted, updated = (
                        self.repository.upsert_staged_establishments(
                            connection,
                            run_id=run_id,
                            source_month=source_month,
                            source_name=self.source_name,
                        )
                        if batch.records
                        else (0, 0, 0)
                    )
                    missing_company = len(batch.records) - candidates
                    errors = list(batch.errors)
                    if missing_company:
                        errors.append(
                            RowError(
                                None,
                                f"{missing_company} estabelecimentos sem Empresa correspondente",
                            )
                        )
                    self._log_errors(
                        connection,
                        run_id,
                        source_month,
                        file_name,
                        errors,
                    )
                    self.repository.advance_file_checkpoint(
                        connection,
                        run_id=run_id,
                        file_name=file_name,
                        processed_rows=batch.processed_rows,
                        total_delta=batch.total_rows,
                        active_delta=len(batch.records),
                        inserted_delta=inserted,
                        updated_delta=updated,
                        error_delta=batch.error_rows + missing_company,
                    )
                LOGGER.info(
                    "%s: %s linhas processadas",
                    file_name,
                    batch.processed_rows,
                )
            with connection.transaction():
                self.repository.mark_file_processed(
                    connection, run_id=run_id, file_name=file_name
                )
            self._delete_zip(result.path)

    def _load_partners(
        self,
        connection,
        *,
        run_id: int,
        source_month: str,
        manifest: SourceManifest,
        files_by_name: dict[str, SourceFile],
    ) -> None:
        del manifest
        for index in range(10):
            file_name = f"Socios{index}.zip"
            source_file = files_by_name[file_name]
            checkpoint = self._get_file_checkpoint(
                connection,
                run_id=run_id,
                file_name=file_name,
            )
            if checkpoint["status"] == "PROCESSED":
                LOGGER.info("%s já processado, pulando", file_name)
                continue

            skip_rows = int(checkpoint.get("processed_rows") or 0)
            result = self._download_one(
                source_month,
                source_file,
                connection=connection,
                run_id=run_id,
            )
            for batch in iter_partner_batches(
                result.path,
                batch_size=self.batch_size,
                skip_rows=skip_rows,
            ):
                with connection.transaction():
                    if batch.records:
                        filtered = self._filter_active_partners(
                            connection,
                            batch.records,
                        )
                        if filtered:
                            self.repository.copy_partners(connection, filtered)
                    self._log_errors(
                        connection,
                        run_id,
                        source_month,
                        file_name,
                        batch.errors,
                    )
                    self.repository.advance_file_checkpoint(
                        connection,
                        run_id=run_id,
                        file_name=file_name,
                        processed_rows=batch.processed_rows,
                        total_delta=batch.total_rows,
                        active_delta=0,
                        inserted_delta=0,
                        updated_delta=0,
                        error_delta=batch.error_rows,
                    )
                LOGGER.info(
                    "%s: %s linhas processadas",
                    file_name,
                    batch.processed_rows,
                )
            with connection.transaction():
                self.repository.mark_file_processed(
                    connection,
                    run_id=run_id,
                    file_name=file_name,
                )
            self._delete_zip(result.path)

    def _build_active_roots_table(self, connection) -> None:
        """Cria tabela temporária UNLOGGED com cnpj_basico de empresas ativas."""
        connection.execute(
            """
            CREATE UNLOGGED TABLE IF NOT EXISTS active_cnpj_roots (
                cnpj_basico CHAR(8) PRIMARY KEY
            )
            """
        )
        connection.execute("TRUNCATE active_cnpj_roots")
        connection.execute(
            """
            INSERT INTO active_cnpj_roots (cnpj_basico)
            SELECT DISTINCT cnpj_basico FROM companies WHERE is_active = TRUE
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_acr_cnpj_basico
            ON active_cnpj_roots (cnpj_basico)
            """
        )

    def _filter_active_partners(
        self,
        connection,
        records: Sequence[PartnerRecord],
    ) -> list[PartnerRecord]:
        if not records:
            return []
        basicos = list({record.cnpj_basico for record in records})
        rows = connection.execute(
            """
            SELECT cnpj_basico
            FROM active_cnpj_roots
            WHERE cnpj_basico = ANY(%s)
            """,
            (basicos,),
        ).fetchall()
        active = {
            row["cnpj_basico"] if isinstance(row, dict) else row[0]
            for row in rows
        }
        return [record for record in records if record.cnpj_basico in active]

    def _finalize_snapshot(
        self,
        connection,
        *,
        run_id: int,
        source_month: str,
    ) -> None:
        with connection.transaction():
            self.repository.reconcile_inactive(
                connection,
                run_id=run_id,
                source_name=self.source_name,
            )
            self.repository.promote_partners(
                connection,
                source_month=source_month,
            )
            self.repository.complete_run(connection, run_id=run_id)
            self.repository.reset_staging(connection)

    def _log_errors(
        self,
        connection,
        run_id: int,
        source_month: str,
        file_name: str,
        errors: Sequence[RowError],
    ) -> None:
        self.repository.log_errors(
            connection,
            run_id=run_id,
            source_month=source_month,
            file_name=file_name,
            errors=[(error.cnpj, error.message) for error in errors],
        )
