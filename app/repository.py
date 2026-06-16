from __future__ import annotations

import re
from typing import Iterable, Sequence

from psycopg import sql

from app.models import (
    AutoRetryState,
    CompanyRecord,
    EstablishmentRecord,
    PartnerRecord,
    SourceFile,
)


class ActiveRunConflict(RuntimeError):
    def __init__(self, active_run: dict) -> None:
        super().__init__("já existe uma importação ativa")
        self.active_run = active_run


COMPANY_CANDIDATE_SQL = """
SELECT
    establishment.cnpj,
    establishment.cnpj_basico,
    establishment.cnpj_ordem,
    establishment.cnpj_dv,
    establishment.is_matriz,
    company.razao_social,
    CASE COALESCE(NULLIF(company.porte_codigo, ''), '00')
        WHEN '01' THEN 'MICRO EMPRESA'
        WHEN '03' THEN 'EMPRESA DE PEQUENO PORTE'
        WHEN '05' THEN 'DEMAIS'
        ELSE 'NÃO INFORMADO'
    END AS porte,
    CASE
        WHEN mei.cnpj_basico IS NOT NULL THEN 'MEI'
        WHEN company.porte_codigo = '01' THEN 'ME'
        WHEN company.porte_codigo = '03' THEN 'EPP'
        WHEN company.porte_codigo = '05' THEN 'DEMAIS'
        ELSE 'NAO_INFORMADO'
    END AS porte_normalizado,
    nature.descricao AS natureza_juridica,
    COALESCE(nature.natureza_grupo, 'OUTROS') AS natureza_grupo,
    company.capital_social,
    establishment.data_abertura,
    establishment.uf,
    municipality.descricao AS cidade,
    establishment.cnae_principal AS cnae_principal_codigo,
    cnae.descricao AS cnae_principal_descricao,
    COALESCE(cnae.categoria_macro, 'Outros') AS categoria_macro,
    cnae.descricao AS categoria_sub,
    establishment.situacao_cadastral
FROM stg_estabelecimentos_ativos AS establishment
JOIN stg_empresas AS company
    ON company.cnpj_basico = establishment.cnpj_basico
LEFT JOIN stg_mei AS mei
    ON mei.cnpj_basico = establishment.cnpj_basico
LEFT JOIN stg_naturezas AS nature
    ON nature.codigo = company.natureza_codigo
LEFT JOIN stg_municipios AS municipality
    ON municipality.codigo = establishment.municipio_codigo
LEFT JOIN stg_cnaes AS cnae
    ON cnae.codigo = establishment.cnae_principal
"""


COMPANY_UPSERT_SQL = f"""
INSERT INTO companies (
    cnpj,
    cnpj_basico,
    cnpj_ordem,
    cnpj_dv,
    is_matriz,
    razao_social,
    porte,
    porte_normalizado,
    natureza_juridica,
    natureza_grupo,
    capital_social,
    data_abertura,
    uf,
    cidade,
    cnae_principal_codigo,
    cnae_principal_descricao,
    categoria_macro,
    categoria_sub,
    last_checked_at,
    is_active,
    inactive_at,
    last_seen_source_month,
    situacao_cadastral
)
SELECT
    candidate.cnpj,
    candidate.cnpj_basico,
    candidate.cnpj_ordem,
    candidate.cnpj_dv,
    candidate.is_matriz,
    candidate.razao_social,
    candidate.porte,
    candidate.porte_normalizado,
    candidate.natureza_juridica,
    candidate.natureza_grupo,
    candidate.capital_social,
    candidate.data_abertura,
    candidate.uf,
    candidate.cidade,
    candidate.cnae_principal_codigo,
    candidate.cnae_principal_descricao,
    candidate.categoria_macro,
    candidate.categoria_sub,
    NOW(),
    TRUE,
    NULL,
    %s,
    'ATIVA'
FROM ({COMPANY_CANDIDATE_SQL}) AS candidate
ON CONFLICT (cnpj)
DO UPDATE SET
    razao_social = EXCLUDED.razao_social,
    cnpj_basico = EXCLUDED.cnpj_basico,
    cnpj_ordem = EXCLUDED.cnpj_ordem,
    cnpj_dv = EXCLUDED.cnpj_dv,
    is_matriz = EXCLUDED.is_matriz,
    porte = EXCLUDED.porte,
    porte_normalizado = EXCLUDED.porte_normalizado,
    natureza_juridica = EXCLUDED.natureza_juridica,
    natureza_grupo = EXCLUDED.natureza_grupo,
    capital_social = EXCLUDED.capital_social,
    data_abertura = EXCLUDED.data_abertura,
    uf = EXCLUDED.uf,
    cidade = EXCLUDED.cidade,
    cnae_principal_codigo = EXCLUDED.cnae_principal_codigo,
    cnae_principal_descricao = EXCLUDED.cnae_principal_descricao,
    categoria_macro = EXCLUDED.categoria_macro,
    categoria_sub = EXCLUDED.categoria_sub,
    updated_at = NOW(),
    last_checked_at = NOW(),
    is_active = TRUE,
    inactive_at = NULL,
    last_seen_source_month = EXCLUDED.last_seen_source_month,
    situacao_cadastral = 'ATIVA'
"""


class ImportRepository:
    def __init__(self, *, error_sample_limit: int = 1000) -> None:
        self.error_sample_limit = error_sample_limit

    @staticmethod
    def _file_type(file_name: str) -> str:
        match = re.match(r"([A-Za-z]+)", file_name)
        return match.group(1).lower() if match else "unknown"

    def create_run(
        self,
        connection,
        *,
        source_month: str,
        source_url: str,
        source_name: str = "casa_dos_dados",
    ) -> int:
        existing = connection.execute(
            """
            SELECT id
            FROM cnpj_import_runs
            WHERE source_name = %s
              AND source_month = %s
              AND status IN ('QUEUED', 'RUNNING')
            ORDER BY id DESC
            LIMIT 1
            """,
            (source_name, source_month),
        ).fetchone()
        if existing:
            return int(existing["id"] if isinstance(existing, dict) else existing[0])
        row = connection.execute(
            """
            INSERT INTO cnpj_import_runs (
                source_name, source_month, source_url, status, phase
            )
            VALUES (%s, %s, %s, 'QUEUED', 'QUEUED')
            RETURNING id
            """,
            (source_name, source_month, source_url),
        ).fetchone()
        return int(row["id"] if isinstance(row, dict) else row[0])

    @staticmethod
    def _active_run_dict(row) -> dict:
        if isinstance(row, dict):
            return {
                "id": int(row["id"]),
                "source_month": row["source_month"],
                "status": row["status"],
                "trigger_type": row["trigger_type"],
            }
        return {
            "id": int(row[0]),
            "source_month": row[1],
            "status": row[2],
            "trigger_type": row[3],
        }

    def get_active_run(self, connection):
        row = connection.execute(
            """
            SELECT id, source_month, status, trigger_type
            FROM cnpj_import_runs
            WHERE status IN ('QUEUED', 'RUNNING')
            ORDER BY requested_at, id
            LIMIT 1
            """
        ).fetchone()
        return self._active_run_dict(row) if row else None

    def recover_stale_runs(
        self,
        connection,
        *,
        timeout_seconds: int,
    ) -> dict[str, int]:
        running = connection.execute(
            """
            UPDATE cnpj_import_runs
            SET status = 'FAILED',
                phase = 'FAILED',
                finished_at = NOW(),
                heartbeat_at = NOW(),
                error_message = 'STALE_RUN_TIMEOUT'
            WHERE status = 'RUNNING'
              AND COALESCE(heartbeat_at, started_at, requested_at)
                  < NOW() - (%s * INTERVAL '1 second')
            """,
            (timeout_seconds,),
        ).rowcount
        queued = connection.execute(
            """
            UPDATE cnpj_import_runs
            SET status = 'FAILED',
                phase = 'FAILED',
                finished_at = NOW(),
                error_message = 'STALE_QUEUE_TIMEOUT'
            WHERE status = 'QUEUED'
              AND requested_at < NOW() - (%s * INTERVAL '1 second')
            """,
            (timeout_seconds,),
        ).rowcount
        return {"running": running, "queued": queued}

    def create_run_exclusive(
        self,
        connection,
        *,
        source_month: str,
        source_url: str,
        trigger_type: str,
        stale_timeout_seconds: int,
        enqueue_lock_id: int,
        import_lock_id: int,
        source_name: str = "casa_dos_dados",
    ) -> int:
        connection.execute(
            "SELECT pg_advisory_xact_lock(%s)",
            (enqueue_lock_id,),
        )
        self.recover_stale_runs(
            connection,
            timeout_seconds=stale_timeout_seconds,
        )
        self.recover_orphaned_runs(connection, lock_id=import_lock_id)
        active_run = self.get_active_run(connection)
        if active_run:
            raise ActiveRunConflict(active_run)
        row = connection.execute(
            """
            INSERT INTO cnpj_import_runs (
                source_name,
                source_month,
                source_url,
                status,
                phase,
                trigger_type
            )
            VALUES (%s, %s, %s, 'QUEUED', 'QUEUED', %s)
            RETURNING id
            """,
            (source_name, source_month, source_url, trigger_type),
        ).fetchone()
        return int(row["id"] if isinstance(row, dict) else row[0])

    def initialize_files(
        self,
        connection,
        *,
        run_id: int,
        files: Sequence[SourceFile],
    ) -> None:
        for source_file in files:
            file_type = self._file_type(source_file.name)
            connection.execute(
                """
                INSERT INTO cnpj_import_files (
                    run_id, file_name, file_type, source_url, etag, content_length
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, file_name)
                DO UPDATE SET
                    source_url = EXCLUDED.source_url,
                    etag = EXCLUDED.etag,
                    content_length = EXCLUDED.content_length
                """,
                (
                    run_id,
                    source_file.name,
                    file_type,
                    source_file.url,
                    source_file.etag,
                    source_file.content_length,
                ),
            )
        connection.execute(
            "UPDATE cnpj_import_runs SET files_total = %s WHERE id = %s",
            (len(files), run_id),
        )

    def claim_next_run(self, connection):
        return connection.execute(
            """
            WITH next_run AS (
                SELECT id
                FROM cnpj_import_runs
                WHERE status = 'QUEUED'
                ORDER BY requested_at, id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE cnpj_import_runs AS run
            SET status = 'RUNNING',
                phase = 'STARTING',
                started_at = COALESCE(started_at, NOW()),
                heartbeat_at = NOW(),
                error_message = NULL
            FROM next_run
            WHERE run.id = next_run.id
            RETURNING run.*
            """
        ).fetchone()

    def start_run(self, connection, *, run_id: int) -> None:
        connection.execute(
            """
            UPDATE cnpj_import_runs
            SET status = 'RUNNING',
                phase = 'STARTING',
                started_at = COALESCE(started_at, NOW()),
                heartbeat_at = NOW(),
                error_message = NULL
            WHERE id = %s AND status = 'QUEUED'
            """,
            (run_id,),
        )

    def requeue_run(self, connection, *, run_id: int) -> None:
        connection.execute(
            """
            UPDATE cnpj_import_runs
            SET status = 'QUEUED', phase = 'WAITING_LOCK', heartbeat_at = NOW()
            WHERE id = %s
            """,
            (run_id,),
        )

    def recover_orphaned_runs(self, connection, *, lock_id: int) -> int:
        locked = connection.execute(
            "SELECT pg_try_advisory_lock(%s) AS locked",
            (lock_id,),
        ).fetchone()
        lock_acquired = bool(
            locked["locked"] if isinstance(locked, dict) else locked[0]
        )
        if not lock_acquired:
            return 0
        try:
            cursor = connection.execute(
                """
                UPDATE cnpj_import_runs
                SET status = 'QUEUED',
                    phase = 'RECOVERED',
                    error_message = NULL
                WHERE status = 'RUNNING'
                """
            )
            return cursor.rowcount
        finally:
            connection.execute(
                "SELECT pg_advisory_unlock(%s)",
                (lock_id,),
            )

    def try_advisory_lock(self, connection, *, lock_id: int) -> bool:
        row = connection.execute(
            "SELECT pg_try_advisory_lock(%s) AS locked",
            (lock_id,),
        ).fetchone()
        return bool(row["locked"] if isinstance(row, dict) else row[0])

    def release_advisory_lock(self, connection, *, lock_id: int) -> None:
        connection.execute(
            "SELECT pg_advisory_unlock(%s)",
            (lock_id,),
        )

    def get_auto_retry_state(
        self,
        connection,
        *,
        source_month: str,
    ) -> AutoRetryState:
        row = connection.execute(
            """
            WITH manual_boundary AS (
                SELECT MAX(requested_at) AS requested_at
                FROM cnpj_import_runs
                WHERE source_month = %s
                  AND trigger_type = 'MANUAL'
            )
            SELECT
                EXISTS (
                    SELECT 1
                    FROM cnpj_import_runs
                    WHERE source_month = %s
                      AND status = 'SUCCEEDED'
                ) AS already_imported,
                COUNT(*) FILTER (
                    WHERE run.trigger_type = 'AUTO'
                      AND run.status = 'FAILED'
                      AND (
                          boundary.requested_at IS NULL
                          OR run.requested_at > boundary.requested_at
                      )
                ) AS failed_auto_attempts,
                MAX(run.finished_at) FILTER (
                    WHERE run.trigger_type = 'AUTO'
                      AND run.status = 'FAILED'
                      AND (
                          boundary.requested_at IS NULL
                          OR run.requested_at > boundary.requested_at
                      )
                ) AS last_auto_failure_at
            FROM cnpj_import_runs AS run
            CROSS JOIN manual_boundary AS boundary
            WHERE run.source_month = %s
            GROUP BY boundary.requested_at
            """,
            (source_month, source_month, source_month),
        ).fetchone()
        if not row:
            return AutoRetryState(False, 0, None)
        if isinstance(row, dict):
            return AutoRetryState(
                bool(row["already_imported"]),
                int(row["failed_auto_attempts"]),
                row["last_auto_failure_at"],
            )
        return AutoRetryState(bool(row[0]), int(row[1]), row[2])

    def get_month_statuses(
        self,
        connection,
        source_months: Sequence[str],
    ) -> dict[str, dict]:
        statuses = {
            source_month: {
                "status": "NOT_IMPORTED",
                "already_imported": False,
            }
            for source_month in source_months
        }
        if not source_months:
            return statuses
        rows = connection.execute(
            """
            WITH ranked AS (
                SELECT
                    source_month,
                    status,
                    ROW_NUMBER() OVER (
                        PARTITION BY source_month
                        ORDER BY requested_at DESC, id DESC
                    ) AS position,
                    BOOL_OR(status = 'SUCCEEDED') OVER (
                        PARTITION BY source_month
                    ) AS already_imported
                FROM cnpj_import_runs
                WHERE source_month = ANY(%s)
            )
            SELECT source_month, status, already_imported
            FROM ranked
            WHERE position = 1
            """,
            (list(source_months),),
        ).fetchall()
        for row in rows:
            source_month = row["source_month"] if isinstance(row, dict) else row[0]
            statuses[source_month] = {
                "status": row["status"] if isinstance(row, dict) else row[1],
                "already_imported": bool(
                    row["already_imported"] if isinstance(row, dict) else row[2]
                ),
            }
        return statuses

    def update_auto_import_state(
        self,
        connection,
        *,
        checked_at,
        result: str | None,
        next_check_at,
        source_month: str | None,
    ) -> None:
        connection.execute(
            """
            INSERT INTO cnpj_auto_import_state (
                id,
                last_auto_check_at,
                last_auto_check_result,
                next_auto_check_at,
                last_detected_source_month,
                updated_at
            )
            VALUES (
                1,
                %s,
                COALESCE(%s, 'DISABLED'),
                %s,
                %s,
                NOW()
            )
            ON CONFLICT (id)
            DO UPDATE SET
                last_auto_check_at = COALESCE(
                    EXCLUDED.last_auto_check_at,
                    cnpj_auto_import_state.last_auto_check_at
                ),
                last_auto_check_result = COALESCE(
                    %s,
                    cnpj_auto_import_state.last_auto_check_result
                ),
                next_auto_check_at = EXCLUDED.next_auto_check_at,
                last_detected_source_month = COALESCE(
                    EXCLUDED.last_detected_source_month,
                    cnpj_auto_import_state.last_detected_source_month
                ),
                updated_at = NOW()
            """,
            (
                checked_at,
                result,
                next_check_at,
                source_month,
                result,
            ),
        )

    def get_auto_import_state(self, connection):
        return connection.execute(
            """
            SELECT
                last_auto_check_at,
                last_auto_check_result,
                next_auto_check_at,
                last_detected_source_month
            FROM cnpj_auto_import_state
            WHERE id = 1
            """
        ).fetchone()

    def get_active_source_months(self, connection) -> set[str]:
        rows = connection.execute(
            """
            SELECT DISTINCT source_month
            FROM cnpj_import_runs
            WHERE status IN ('QUEUED', 'RUNNING')
              AND source_month IS NOT NULL
            """
        ).fetchall()
        return {
            row["source_month"] if isinstance(row, dict) else row[0]
            for row in rows
        }

    def get_run(self, connection, run_id: int):
        return connection.execute(
            "SELECT * FROM cnpj_import_runs WHERE id = %s",
            (run_id,),
        ).fetchone()

    def list_runs(self, connection, *, limit: int, offset: int):
        return connection.execute(
            """
            SELECT *
            FROM cnpj_import_runs
            ORDER BY requested_at DESC, id DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        ).fetchall()

    def get_file(self, connection, *, run_id: int, file_name: str):
        return connection.execute(
            """
            SELECT *
            FROM cnpj_import_files
            WHERE run_id = %s AND file_name = %s
            """,
            (run_id, file_name),
        ).fetchone()

    def mark_downloaded(
        self,
        connection,
        *,
        run_id: int,
        file_name: str,
        etag: str | None,
        content_length: int | None,
    ) -> None:
        connection.execute(
            """
            UPDATE cnpj_import_files
            SET status = CASE WHEN status = 'PROCESSED' THEN status ELSE 'DOWNLOADED' END,
                etag = %s,
                content_length = %s,
                downloaded_at = NOW(),
                error_message = NULL
            WHERE run_id = %s AND file_name = %s
            """,
            (etag, content_length, run_id, file_name),
        )

    def reset_file_checkpoint(
        self,
        connection,
        *,
        run_id: int,
        file_name: str,
    ) -> None:
        connection.execute(
            """
            UPDATE cnpj_import_files
            SET status = 'DOWNLOADED',
                processed_rows = 0,
                total_rows = 0,
                active_rows = 0,
                inserted_rows = 0,
                updated_rows = 0,
                error_rows = 0,
                started_at = NULL,
                finished_at = NULL,
                error_message = NULL
            WHERE run_id = %s AND file_name = %s
            """,
            (run_id, file_name),
        )

    def reset_staging(self, connection) -> None:
        connection.execute(
            """
            TRUNCATE
                stg_empresas,
                stg_mei,
                stg_naturezas,
                stg_municipios,
                stg_cnaes,
                stg_estabelecimentos_ativos,
                stg_qualificacoes,
                stg_company_partners
            """
        )

    def truncate_company_staging(self, connection) -> None:
        """Libera staging de empresas e auxiliares após Fase 3.

        Preserva stg_qualificacoes, necessária para promote_partners.
        """
        connection.execute(
            """
            TRUNCATE
                stg_empresas,
                stg_mei,
                stg_naturezas,
                stg_municipios,
                stg_cnaes
            """
        )

    def _copy_rows(
        self,
        connection,
        table_name: str,
        columns: Sequence[str],
        rows: Iterable[Sequence],
    ) -> None:
        statement = sql.SQL("COPY {} ({}) FROM STDIN").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
        )
        with connection.cursor().copy(statement) as copy:
            for row in rows:
                copy.write_row(row)

    def copy_companies(self, connection, rows: Iterable[CompanyRecord]) -> None:
        self._copy_rows(
            connection,
            "stg_empresas",
            ("cnpj_basico", "razao_social", "natureza_codigo", "capital_social", "porte_codigo"),
            (
                (
                    row.cnpj_basico,
                    row.razao_social,
                    row.natureza_codigo,
                    row.capital_social,
                    row.porte_codigo,
                )
                for row in rows
            ),
        )

    def copy_reference_rows(
        self,
        connection,
        *,
        table_name: str,
        columns: Sequence[str],
        rows: Iterable[Sequence],
    ) -> None:
        self._copy_rows(connection, table_name, columns, rows)

    def copy_establishments(
        self,
        connection,
        rows: Iterable[EstablishmentRecord],
    ) -> None:
        connection.execute("TRUNCATE stg_estabelecimentos_ativos")
        self._copy_rows(
            connection,
            "stg_estabelecimentos_ativos",
            (
                "cnpj",
                "cnpj_basico",
                "cnpj_ordem",
                "cnpj_dv",
                "is_matriz",
                "data_abertura",
                "cnae_principal",
                "uf",
                "municipio_codigo",
                "situacao_cadastral",
            ),
            (
                (
                    row.cnpj,
                    row.cnpj_basico,
                    row.cnpj_ordem or row.cnpj[8:12],
                    row.cnpj_dv or row.cnpj[12:14],
                    (row.cnpj_ordem or row.cnpj[8:12]) == "0001",
                    row.data_abertura,
                    row.cnae_principal,
                    row.uf,
                    row.municipio_codigo,
                    row.situacao_cadastral,
                )
                for row in rows
            ),
        )

    def copy_partners(
        self,
        connection,
        rows: Iterable[PartnerRecord],
    ) -> None:
        self._copy_rows(
            connection,
            "stg_company_partners",
            (
                "cnpj_basico",
                "partner_identifier",
                "partner_name",
                "partner_document",
                "partner_qualification_code",
                "entry_date",
                "country_code",
                "legal_representative_document",
                "legal_representative_name",
                "legal_representative_qualification_code",
                "age_range_code",
                "age_range",
            ),
            (
                (
                    row.cnpj_basico,
                    row.partner_identifier,
                    row.partner_name,
                    row.partner_document,
                    row.partner_qualification_code,
                    row.entry_date,
                    row.country_code,
                    row.legal_representative_document,
                    row.legal_representative_name,
                    row.legal_representative_qualification_code,
                    row.age_range_code,
                    row.age_range,
                )
                for row in rows
            ),
        )

    def upsert_staged_establishments(
        self,
        connection,
        *,
        run_id: int,
        source_month: str,
        source_name: str,
    ) -> tuple[int, int, int]:
        counts = connection.execute(
            f"""
            SELECT
                COUNT(*) AS candidates,
                COUNT(*) FILTER (WHERE existing.cnpj IS NULL) AS inserted
            FROM ({COMPANY_CANDIDATE_SQL}) AS candidate
            LEFT JOIN companies AS existing ON existing.cnpj = candidate.cnpj
            """
        ).fetchone()
        candidates = int(counts["candidates"] if isinstance(counts, dict) else counts[0])
        inserted = int(counts["inserted"] if isinstance(counts, dict) else counts[1])
        connection.execute(COMPANY_UPSERT_SQL, (source_month,))
        connection.execute(
            """
            INSERT INTO cnpj_import_membership (
                source_name, cnpj, last_seen_run_id, last_seen_source_month, updated_at
            )
            SELECT %s, candidate.cnpj, %s, %s, NOW()
            FROM ("""
            + COMPANY_CANDIDATE_SQL
            + """) AS candidate
            ON CONFLICT (source_name, cnpj)
            DO UPDATE SET
                last_seen_run_id = EXCLUDED.last_seen_run_id,
                last_seen_source_month = EXCLUDED.last_seen_source_month,
                updated_at = NOW()
            """,
            (source_name, run_id, source_month),
        )
        return candidates, inserted, candidates - inserted

    def advance_file_checkpoint(
        self,
        connection,
        *,
        run_id: int,
        file_name: str,
        processed_rows: int,
        total_delta: int,
        active_delta: int,
        inserted_delta: int,
        updated_delta: int,
        error_delta: int,
    ) -> None:
        connection.execute(
            """
            UPDATE cnpj_import_files
            SET status = 'PROCESSING',
                processed_rows = %s,
                total_rows = total_rows + %s,
                active_rows = active_rows + %s,
                inserted_rows = inserted_rows + %s,
                updated_rows = updated_rows + %s,
                error_rows = error_rows + %s,
                started_at = COALESCE(started_at, NOW()),
                error_message = NULL
            WHERE run_id = %s AND file_name = %s
            """,
            (
                processed_rows,
                total_delta,
                active_delta,
                inserted_delta,
                updated_delta,
                error_delta,
                run_id,
                file_name,
            ),
        )
        self.touch_run(
            connection,
            run_id=run_id,
            phase=f"PROCESSING:{file_name}",
            file_name=file_name,
        )

    def mark_file_processed(self, connection, *, run_id: int, file_name: str) -> None:
        connection.execute(
            """
            UPDATE cnpj_import_files
            SET status = 'PROCESSED', finished_at = NOW(), error_message = NULL
            WHERE run_id = %s AND file_name = %s
            """,
            (run_id, file_name),
        )
        connection.execute(
            """
            UPDATE cnpj_import_runs
            SET files_completed = (
                SELECT COUNT(*) FROM cnpj_import_files
                WHERE run_id = %s AND status = 'PROCESSED'
            )
            WHERE id = %s
            """,
            (run_id, run_id),
        )

    def log_errors(
        self,
        connection,
        *,
        run_id: int,
        source_month: str,
        file_name: str,
        errors: Sequence[tuple[str | None, str]],
    ) -> None:
        if not errors or self.error_sample_limit <= 0:
            return
        sampled = connection.execute(
            "SELECT COUNT(*) FROM cnpj_import_errors WHERE run_id = %s",
            (run_id,),
        ).fetchone()
        count = int(sampled["count"] if isinstance(sampled, dict) else sampled[0])
        remaining = self.error_sample_limit - count
        if remaining <= 0:
            return
        query = """
            INSERT INTO cnpj_import_errors (
                run_id, source_month, file_name, cnpj, error_message
            )
            VALUES (%s, %s, %s, %s, %s)
        """
        params = [
            (
                run_id,
                source_month,
                file_name,
                cnpj,
                message[:1000],
            )
            for cnpj, message in errors[:remaining]
        ]
        with connection.cursor() as cursor:
            cursor.executemany(query, params)

    def touch_run(
        self,
        connection,
        *,
        run_id: int,
        phase: str,
        file_name: str | None = None,
    ) -> None:
        connection.execute(
            """
            UPDATE cnpj_import_runs
            SET phase = %s,
                file_name = %s,
                heartbeat_at = NOW()
            WHERE id = %s
            """,
            (phase, file_name, run_id),
        )

    def heartbeat_run(self, connection, *, run_id: int) -> None:
        connection.execute(
            """
            UPDATE cnpj_import_runs
            SET heartbeat_at = NOW()
            WHERE id = %s AND status = 'RUNNING'
            """,
            (run_id,),
        )

    def reconcile_inactive(
        self,
        connection,
        *,
        run_id: int,
        source_name: str,
    ) -> int:
        cursor = connection.execute(
            """
            UPDATE companies AS c
            SET is_active = FALSE,
                inactive_at = CASE WHEN c.is_active THEN NOW() ELSE c.inactive_at END,
                updated_at = CASE WHEN c.is_active THEN NOW() ELSE c.updated_at END
            FROM cnpj_import_membership AS membership
            WHERE membership.source_name = %s
              AND membership.cnpj = c.cnpj
              AND membership.last_seen_run_id <> %s
              AND c.is_active = TRUE
            """,
            (source_name, run_id),
        )
        return cursor.rowcount

    def promote_partners(self, connection, *, source_month: str) -> None:
        connection.execute("TRUNCATE company_partners")
        connection.execute(
            """
            INSERT INTO company_partners (
                cnpj_basico,
                partner_identifier,
                partner_name,
                partner_document,
                partner_qualification_code,
                partner_qualification,
                entry_date,
                country_code,
                legal_representative_document,
                legal_representative_name,
                legal_representative_qualification_code,
                legal_representative_qualification,
                age_range_code,
                age_range,
                source_month
            )
            SELECT
                partner.cnpj_basico,
                partner.partner_identifier,
                partner.partner_name,
                partner.partner_document,
                partner.partner_qualification_code,
                qualification.descricao,
                partner.entry_date,
                partner.country_code,
                partner.legal_representative_document,
                partner.legal_representative_name,
                partner.legal_representative_qualification_code,
                representative_qualification.descricao,
                partner.age_range_code,
                partner.age_range,
                %s
            FROM stg_company_partners AS partner
            LEFT JOIN stg_qualificacoes AS qualification
                ON qualification.codigo = partner.partner_qualification_code
            LEFT JOIN stg_qualificacoes AS representative_qualification
                ON representative_qualification.codigo =
                   partner.legal_representative_qualification_code
            WHERE EXISTS (
                SELECT 1
                FROM companies AS company
                WHERE company.is_active = TRUE
                  AND company.cnpj_basico = partner.cnpj_basico
            )
            """,
            (source_month,),
        )

    def complete_run(self, connection, *, run_id: int) -> None:
        connection.execute(
            """
            UPDATE cnpj_import_runs AS run
            SET status = 'SUCCEEDED',
                phase = 'COMPLETED',
                total_rows = totals.total_rows,
                active_rows = totals.active_rows,
                inserted_rows = totals.inserted_rows,
                updated_rows = totals.updated_rows,
                error_rows = totals.error_rows,
                heartbeat_at = NOW(),
                finished_at = NOW(),
                error_message = NULL
            FROM (
                SELECT
                    COALESCE(SUM(total_rows), 0) AS total_rows,
                    COALESCE(SUM(active_rows), 0) AS active_rows,
                    COALESCE(SUM(inserted_rows), 0) AS inserted_rows,
                    COALESCE(SUM(updated_rows), 0) AS updated_rows,
                    COALESCE(SUM(error_rows), 0) AS error_rows
                FROM cnpj_import_files
                WHERE run_id = %s AND file_type = 'estabelecimentos'
            ) AS totals
            WHERE run.id = %s
            """,
            (run_id, run_id),
        )

    def fail_run(self, connection, *, run_id: int, error_message: str) -> None:
        connection.execute(
            """
            UPDATE cnpj_import_runs
            SET status = 'FAILED',
                phase = 'FAILED',
                heartbeat_at = NOW(),
                finished_at = NOW(),
                error_message = %s
            WHERE id = %s
            """,
            (error_message[:4000], run_id),
        )

    def stats(self, connection):
        total = connection.execute("SELECT COUNT(*) AS count FROM companies").fetchone()
        active = connection.execute(
            "SELECT COUNT(*) AS count FROM companies WHERE is_active = TRUE"
        ).fetchone()
        total_partners = connection.execute(
            "SELECT COUNT(*) AS count FROM company_partners"
        ).fetchone()
        partners_by_qualification = connection.execute(
            """
            SELECT
                partner_qualification_code,
                partner_qualification,
                COUNT(*) AS total
            FROM company_partners
            GROUP BY partner_qualification_code, partner_qualification
            ORDER BY total DESC, partner_qualification_code
            """
        ).fetchall()
        by_uf = connection.execute(
            """
            SELECT uf, COUNT(*) AS total
            FROM companies
            WHERE is_active = TRUE
            GROUP BY uf
            ORDER BY total DESC
            """
        ).fetchall()
        by_category = connection.execute(
            """
            SELECT categoria_macro, COUNT(*) AS total
            FROM companies
            WHERE is_active = TRUE
            GROUP BY categoria_macro
            ORDER BY total DESC
            """
        ).fetchall()
        last_run = connection.execute(
            """
            SELECT *
            FROM cnpj_import_runs
            ORDER BY requested_at DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
        return {
            "total_companies": total["count"] if isinstance(total, dict) else total[0],
            "active_companies": active["count"] if isinstance(active, dict) else active[0],
            "total_partners": (
                total_partners["count"]
                if isinstance(total_partners, dict)
                else total_partners[0]
            ),
            "partners_by_qualification": partners_by_qualification,
            "by_uf": by_uf,
            "by_category": by_category,
            "last_import": last_run,
        }
