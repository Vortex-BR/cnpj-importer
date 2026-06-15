DROP VIEW IF EXISTS company_partners_full;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'company_partners'
          AND column_name = 'partner_document_masked'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'company_partners'
          AND column_name = 'partner_document'
    ) THEN
        ALTER TABLE company_partners
            RENAME COLUMN partner_document_masked TO partner_document;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'company_partners'
          AND column_name = 'legal_representative_document_masked'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'company_partners'
          AND column_name = 'legal_representative_document'
    ) THEN
        ALTER TABLE company_partners
            RENAME COLUMN legal_representative_document_masked
            TO legal_representative_document;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'stg_company_partners'
          AND column_name = 'partner_document_masked'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'stg_company_partners'
          AND column_name = 'partner_document'
    ) THEN
        ALTER TABLE stg_company_partners
            RENAME COLUMN partner_document_masked TO partner_document;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'stg_company_partners'
          AND column_name = 'legal_representative_document_masked'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'stg_company_partners'
          AND column_name = 'legal_representative_document'
    ) THEN
        ALTER TABLE stg_company_partners
            RENAME COLUMN legal_representative_document_masked
            TO legal_representative_document;
    END IF;
END
$$;

ALTER TABLE company_partners
    DROP CONSTRAINT IF EXISTS company_partners_partner_document_masked_check;
ALTER TABLE company_partners
    DROP CONSTRAINT IF EXISTS
        company_partners_legal_representative_document_masked_check;
ALTER TABLE stg_company_partners
    DROP CONSTRAINT IF EXISTS stg_company_partners_partner_document_masked_check;
ALTER TABLE stg_company_partners
    DROP CONSTRAINT IF EXISTS
        stg_company_partners_legal_representative_document_masked_check;

DROP INDEX IF EXISTS idx_company_partners_document_masked;
CREATE INDEX IF NOT EXISTS idx_company_partners_document
    ON company_partners (partner_document);

CREATE VIEW company_partners_full AS
SELECT
    company.cnpj AS company_cnpj,
    company.razao_social,
    company.uf,
    company.cidade,
    company.porte_normalizado,
    company.categoria_macro,
    partner.partner_name,
    partner.partner_document,
    partner.partner_qualification,
    partner.entry_date,
    partner.age_range,
    partner.source_month
FROM companies AS company
JOIN company_partners AS partner
    ON company.cnpj_basico = partner.cnpj_basico
WHERE company.is_active = TRUE;

