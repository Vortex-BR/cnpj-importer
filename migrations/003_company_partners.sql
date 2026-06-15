ALTER TABLE companies ADD COLUMN IF NOT EXISTS cnpj_basico CHAR(8);
ALTER TABLE companies ADD COLUMN IF NOT EXISTS cnpj_ordem CHAR(4);
ALTER TABLE companies ADD COLUMN IF NOT EXISTS cnpj_dv CHAR(2);
ALTER TABLE companies ADD COLUMN IF NOT EXISTS is_matriz BOOLEAN;

UPDATE companies
SET cnpj_basico = SUBSTRING(cnpj FROM 1 FOR 8),
    cnpj_ordem = SUBSTRING(cnpj FROM 9 FOR 4),
    cnpj_dv = SUBSTRING(cnpj FROM 13 FOR 2),
    is_matriz = SUBSTRING(cnpj FROM 9 FOR 4) = '0001'
WHERE cnpj_basico IS NULL
   OR cnpj_ordem IS NULL
   OR cnpj_dv IS NULL
   OR is_matriz IS NULL;

ALTER TABLE companies ALTER COLUMN is_matriz SET DEFAULT FALSE;
ALTER TABLE companies ALTER COLUMN is_matriz SET NOT NULL;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'companies_cnpj_digits_check'
          AND conrelid = 'companies'::regclass
          AND pg_get_constraintdef(oid) NOT LIKE '%A-Z0-9%'
    ) THEN
        ALTER TABLE companies DROP CONSTRAINT companies_cnpj_digits_check;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'companies_cnpj_digits_check'
          AND conrelid = 'companies'::regclass
    ) THEN
        ALTER TABLE companies
            ADD CONSTRAINT companies_cnpj_digits_check
            CHECK (cnpj ~ '^[A-Z0-9]{12}[0-9]{2}$');
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'companies_is_matriz_check'
          AND conrelid = 'companies'::regclass
    ) THEN
        ALTER TABLE companies
            ADD CONSTRAINT companies_is_matriz_check
            CHECK (
                cnpj_ordem IS NULL
                OR is_matriz = (cnpj_ordem = '0001')
            );
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_companies_active_cnpj_basico
    ON companies (is_active, cnpj_basico);

ALTER TABLE stg_estabelecimentos_ativos
    ADD COLUMN IF NOT EXISTS cnpj_ordem CHAR(4);
ALTER TABLE stg_estabelecimentos_ativos
    ADD COLUMN IF NOT EXISTS cnpj_dv CHAR(2);
ALTER TABLE stg_estabelecimentos_ativos
    ADD COLUMN IF NOT EXISTS is_matriz BOOLEAN;

CREATE TABLE IF NOT EXISTS company_partners (
    cnpj_basico CHAR(8) NOT NULL,
    partner_identifier TEXT,
    partner_name TEXT NOT NULL,
    partner_document_masked TEXT,
    partner_qualification_code TEXT,
    partner_qualification TEXT,
    entry_date DATE,
    country_code TEXT,
    legal_representative_document_masked TEXT,
    legal_representative_name TEXT,
    legal_representative_qualification_code TEXT,
    legal_representative_qualification TEXT,
    age_range_code TEXT,
    age_range TEXT,
    source_month TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_company_partners_cnpj_basico
    ON company_partners (cnpj_basico);
CREATE INDEX IF NOT EXISTS idx_company_partners_partner_name
    ON company_partners (partner_name);
CREATE INDEX IF NOT EXISTS idx_company_partners_qualification_code
    ON company_partners (partner_qualification_code);

CREATE UNLOGGED TABLE IF NOT EXISTS stg_qualificacoes (
    codigo TEXT PRIMARY KEY,
    descricao TEXT NOT NULL
);

CREATE UNLOGGED TABLE IF NOT EXISTS stg_company_partners (
    cnpj_basico CHAR(8) NOT NULL,
    partner_identifier TEXT,
    partner_name TEXT NOT NULL,
    partner_document_masked TEXT,
    partner_qualification_code TEXT,
    entry_date DATE,
    country_code TEXT,
    legal_representative_document_masked TEXT,
    legal_representative_name TEXT,
    legal_representative_qualification_code TEXT,
    age_range_code TEXT,
    age_range TEXT
);
