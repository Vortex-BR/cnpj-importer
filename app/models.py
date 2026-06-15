from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True)
class Porte:
    description: str
    normalized: str


@dataclass(frozen=True)
class CompanyRecord:
    cnpj_basico: str
    razao_social: str
    natureza_codigo: str
    capital_social: str | None
    porte_codigo: str


@dataclass(frozen=True)
class EstablishmentRecord:
    cnpj: str
    cnpj_basico: str
    data_abertura: date | None
    cnae_principal: str
    uf: str | None
    municipio_codigo: str
    situacao_cadastral: str = "ATIVA"
    cnpj_ordem: str = ""
    cnpj_dv: str = ""
    is_matriz: bool = False


@dataclass(frozen=True)
class PartnerRecord:
    cnpj_basico: str
    partner_identifier: str | None
    partner_name: str
    partner_document: str | None
    partner_qualification_code: str | None
    entry_date: date | None
    country_code: str | None
    legal_representative_document: str | None
    legal_representative_name: str | None
    legal_representative_qualification_code: str | None
    age_range_code: str | None
    age_range: str | None


@dataclass(frozen=True)
class SourceFile:
    name: str
    url: str
    content_length: int | None = None
    etag: str | None = None


@dataclass(frozen=True)
class SourceMonth:
    source_month: str
    last_modified: datetime | None
    is_complete: bool


@dataclass(frozen=True)
class AutoRetryState:
    already_imported: bool
    failed_auto_attempts: int
    last_auto_failure_at: datetime | None


@dataclass(frozen=True)
class AutoImportCheck:
    result: str
    source_month: str | None
    run_id: int | None
