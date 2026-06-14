ALTER TABLE cnpj_import_runs
    ADD COLUMN IF NOT EXISTS trigger_type TEXT NOT NULL DEFAULT 'MANUAL';

UPDATE cnpj_import_runs
SET trigger_type = 'MANUAL'
WHERE trigger_type IS NULL OR trigger_type NOT IN ('MANUAL', 'AUTO');

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'cnpj_import_runs_trigger_type_check'
          AND conrelid = 'cnpj_import_runs'::regclass
    ) THEN
        ALTER TABLE cnpj_import_runs
            ADD CONSTRAINT cnpj_import_runs_trigger_type_check
            CHECK (trigger_type IN ('MANUAL', 'AUTO'));
    END IF;
END
$$;

WITH active_runs AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            ORDER BY
                CASE status WHEN 'RUNNING' THEN 0 ELSE 1 END,
                requested_at,
                id
        ) AS position
    FROM cnpj_import_runs
    WHERE status IN ('QUEUED', 'RUNNING')
)
UPDATE cnpj_import_runs AS run
SET status = 'FAILED',
    phase = 'FAILED',
    finished_at = NOW(),
    error_message = 'DUPLICATE_ACTIVE_RUN_RECOVERED'
FROM active_runs
WHERE run.id = active_runs.id
  AND active_runs.position > 1;

CREATE UNIQUE INDEX IF NOT EXISTS ux_cnpj_import_runs_one_active
    ON cnpj_import_runs ((1))
    WHERE status IN ('QUEUED', 'RUNNING');

CREATE INDEX IF NOT EXISTS idx_cnpj_import_runs_month_trigger
    ON cnpj_import_runs (source_month, trigger_type, requested_at DESC);

CREATE TABLE IF NOT EXISTS cnpj_auto_import_state (
    id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    last_auto_check_at TIMESTAMP,
    last_auto_check_result TEXT NOT NULL DEFAULT 'DISABLED',
    next_auto_check_at TIMESTAMP,
    last_detected_source_month TEXT,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

INSERT INTO cnpj_auto_import_state (id)
VALUES (1)
ON CONFLICT (id) DO NOTHING;

