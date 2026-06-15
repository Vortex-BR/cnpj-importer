-- Índice em stg_company_partners para acelerar a promoção final
CREATE INDEX IF NOT EXISTS idx_stg_cp_cnpj_basico
    ON stg_company_partners (cnpj_basico);

-- Tabela UNLOGGED para filtro de sócios por empresa ativa (criada e dropada
-- em runtime, mas garantir que não existe estado residual entre runs)
DROP TABLE IF EXISTS active_cnpj_roots;
