CREATE TABLE IF NOT EXISTS companies (
    cnpj CHAR(14) PRIMARY KEY,
    razao_social TEXT NOT NULL,
    porte TEXT,
    porte_normalizado TEXT,
    natureza_juridica TEXT,
    natureza_grupo TEXT,
    capital_social NUMERIC(18,2),
    data_abertura DATE,
    uf CHAR(2),
    cidade TEXT,
    cnae_principal_codigo VARCHAR(20),
    cnae_principal_descricao TEXT,
    categoria_macro TEXT,
    categoria_sub TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_checked_at TIMESTAMP,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    inactive_at TIMESTAMP,
    last_seen_source_month TEXT,
    situacao_cadastral TEXT
);

ALTER TABLE companies ADD COLUMN IF NOT EXISTS porte TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS porte_normalizado TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS natureza_juridica TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS natureza_grupo TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS capital_social NUMERIC(18,2);
ALTER TABLE companies ADD COLUMN IF NOT EXISTS data_abertura DATE;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS uf CHAR(2);
ALTER TABLE companies ADD COLUMN IF NOT EXISTS cidade TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS cnae_principal_codigo VARCHAR(20);
ALTER TABLE companies ADD COLUMN IF NOT EXISTS cnae_principal_descricao TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS categoria_macro TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS categoria_sub TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT NOW();
ALTER TABLE companies ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT NOW();
ALTER TABLE companies ADD COLUMN IF NOT EXISTS last_checked_at TIMESTAMP;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS inactive_at TIMESTAMP;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS last_seen_source_month TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS situacao_cadastral TEXT;

UPDATE companies SET is_active = TRUE WHERE is_active IS NULL;
ALTER TABLE companies ALTER COLUMN is_active SET DEFAULT TRUE;
ALTER TABLE companies ALTER COLUMN is_active SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_companies_cnpj
    ON companies (cnpj);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'companies_cnpj_digits_check'
    ) THEN
        ALTER TABLE companies
            ADD CONSTRAINT companies_cnpj_digits_check
            CHECK (cnpj ~ '^[0-9]{14}$');
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'companies_uf_check'
    ) THEN
        ALTER TABLE companies
            ADD CONSTRAINT companies_uf_check
            CHECK (uf IS NULL OR uf ~ '^[A-Z]{2}$');
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_companies_categoria_macro
    ON companies (categoria_macro);
CREATE INDEX IF NOT EXISTS idx_companies_uf_cidade
    ON companies (uf, cidade);
CREATE INDEX IF NOT EXISTS idx_companies_porte_normalizado
    ON companies (porte_normalizado);
CREATE INDEX IF NOT EXISTS idx_companies_natureza_grupo
    ON companies (natureza_grupo);
CREATE INDEX IF NOT EXISTS idx_companies_data_abertura
    ON companies (data_abertura);
CREATE INDEX IF NOT EXISTS idx_companies_capital_social
    ON companies (capital_social);
CREATE INDEX IF NOT EXISTS idx_companies_cnae_codigo
    ON companies (cnae_principal_codigo);
CREATE INDEX IF NOT EXISTS idx_companies_is_active
    ON companies (is_active);
CREATE INDEX IF NOT EXISTS idx_companies_last_seen_source_month
    ON companies (last_seen_source_month);

CREATE TABLE IF NOT EXISTS cnpj_import_runs (
    id BIGSERIAL PRIMARY KEY,
    source_name TEXT NOT NULL DEFAULT 'casa_dos_dados',
    source_month TEXT,
    source_url TEXT,
    file_name TEXT,
    status TEXT NOT NULL DEFAULT 'STARTED',
    phase TEXT,
    total_rows BIGINT DEFAULT 0,
    active_rows BIGINT DEFAULT 0,
    inserted_rows BIGINT DEFAULT 0,
    updated_rows BIGINT DEFAULT 0,
    error_rows BIGINT DEFAULT 0,
    files_total INTEGER DEFAULT 0,
    files_completed INTEGER DEFAULT 0,
    requested_at TIMESTAMP NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP,
    heartbeat_at TIMESTAMP,
    finished_at TIMESTAMP,
    error_message TEXT
);

ALTER TABLE cnpj_import_runs ADD COLUMN IF NOT EXISTS source_url TEXT;
ALTER TABLE cnpj_import_runs ADD COLUMN IF NOT EXISTS phase TEXT;
ALTER TABLE cnpj_import_runs ADD COLUMN IF NOT EXISTS files_total INTEGER DEFAULT 0;
ALTER TABLE cnpj_import_runs ADD COLUMN IF NOT EXISTS files_completed INTEGER DEFAULT 0;
ALTER TABLE cnpj_import_runs ADD COLUMN IF NOT EXISTS requested_at TIMESTAMP NOT NULL DEFAULT NOW();
ALTER TABLE cnpj_import_runs ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_cnpj_import_runs_status
    ON cnpj_import_runs (status, requested_at);
CREATE INDEX IF NOT EXISTS idx_cnpj_import_runs_month
    ON cnpj_import_runs (source_month, requested_at DESC);

CREATE TABLE IF NOT EXISTS cnpj_import_errors (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT REFERENCES cnpj_import_runs(id) ON DELETE CASCADE,
    source_month TEXT,
    file_name TEXT,
    cnpj CHAR(14),
    error_message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

ALTER TABLE cnpj_import_errors ADD COLUMN IF NOT EXISTS run_id BIGINT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'cnpj_import_errors_run_id_fkey'
          AND conrelid = 'cnpj_import_errors'::regclass
    ) THEN
        ALTER TABLE cnpj_import_errors
            ADD CONSTRAINT cnpj_import_errors_run_id_fkey
            FOREIGN KEY (run_id)
            REFERENCES cnpj_import_runs(id)
            ON DELETE CASCADE;
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_cnpj_import_errors_run
    ON cnpj_import_errors (run_id, created_at);

CREATE TABLE IF NOT EXISTS cnpj_import_files (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES cnpj_import_runs(id) ON DELETE CASCADE,
    file_name TEXT NOT NULL,
    file_type TEXT NOT NULL,
    source_url TEXT NOT NULL,
    etag TEXT,
    content_length BIGINT,
    status TEXT NOT NULL DEFAULT 'PENDING',
    processed_rows BIGINT NOT NULL DEFAULT 0,
    total_rows BIGINT NOT NULL DEFAULT 0,
    active_rows BIGINT NOT NULL DEFAULT 0,
    inserted_rows BIGINT NOT NULL DEFAULT 0,
    updated_rows BIGINT NOT NULL DEFAULT 0,
    error_rows BIGINT NOT NULL DEFAULT 0,
    started_at TIMESTAMP,
    downloaded_at TIMESTAMP,
    finished_at TIMESTAMP,
    error_message TEXT,
    UNIQUE (run_id, file_name)
);

CREATE INDEX IF NOT EXISTS idx_cnpj_import_files_run_status
    ON cnpj_import_files (run_id, status);

CREATE TABLE IF NOT EXISTS cnpj_import_membership (
    source_name TEXT NOT NULL,
    cnpj CHAR(14) NOT NULL,
    last_seen_run_id BIGINT NOT NULL,
    last_seen_source_month TEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_name, cnpj)
);

CREATE INDEX IF NOT EXISTS idx_cnpj_membership_last_seen_run
    ON cnpj_import_membership (source_name, last_seen_run_id);

CREATE UNLOGGED TABLE IF NOT EXISTS stg_empresas (
    cnpj_basico CHAR(8) PRIMARY KEY,
    razao_social TEXT NOT NULL,
    natureza_codigo VARCHAR(10),
    capital_social NUMERIC(18,2),
    porte_codigo VARCHAR(2)
);

CREATE UNLOGGED TABLE IF NOT EXISTS stg_mei (
    cnpj_basico CHAR(8) PRIMARY KEY
);

CREATE UNLOGGED TABLE IF NOT EXISTS stg_naturezas (
    codigo VARCHAR(10) PRIMARY KEY,
    descricao TEXT NOT NULL,
    natureza_grupo TEXT NOT NULL
);

CREATE UNLOGGED TABLE IF NOT EXISTS stg_municipios (
    codigo VARCHAR(10) PRIMARY KEY,
    descricao TEXT NOT NULL
);

CREATE UNLOGGED TABLE IF NOT EXISTS stg_cnaes (
    codigo VARCHAR(20) PRIMARY KEY,
    descricao TEXT NOT NULL,
    categoria_macro TEXT NOT NULL
);

CREATE UNLOGGED TABLE IF NOT EXISTS stg_estabelecimentos_ativos (
    cnpj CHAR(14) PRIMARY KEY,
    cnpj_basico CHAR(8) NOT NULL,
    data_abertura DATE,
    cnae_principal VARCHAR(20),
    uf CHAR(2),
    municipio_codigo VARCHAR(10),
    situacao_cadastral TEXT NOT NULL
);
