from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path
from typing import Iterator, Sequence

from app.models import CompanyRecord, EstablishmentRecord, PartnerRecord
from app.normalization import (
    AGE_RANGE_MAP,
    build_cnpj,
    parse_capital_social,
    parse_receita_date,
)


def stream_zip_rows(path: str | Path) -> Iterator[list[str]]:
    with zipfile.ZipFile(path) as archive:
        members = [item for item in archive.infolist() if not item.is_dir()]
        if not members:
            raise ValueError(f"ZIP sem CSV: {path}")
        with archive.open(members[0], "r") as raw:
            with io.TextIOWrapper(raw, encoding="latin-1", newline="") as text:
                yield from csv.reader(text, delimiter=";", quotechar='"')


def parse_company_row(row: Sequence[str]) -> CompanyRecord:
    if len(row) < 6:
        raise ValueError("linha de Empresas com menos de 6 colunas")
    cnpj_basico = row[0].strip()
    if len(cnpj_basico) != 8 or not cnpj_basico.isdigit():
        raise ValueError("cnpj_basico inválido")
    razao_social = row[1].strip()
    if not razao_social:
        raise ValueError("razão social vazia")
    capital = parse_capital_social(row[4])
    return CompanyRecord(
        cnpj_basico=cnpj_basico,
        razao_social=razao_social,
        natureza_codigo=row[2].strip(),
        capital_social=format(capital, "f") if capital is not None else None,
        porte_codigo=row[5].strip(),
    )


def parse_establishment_row(row: Sequence[str]) -> EstablishmentRecord | None:
    if len(row) < 21:
        raise ValueError("linha de Estabelecimentos com menos de 21 colunas")
    if row[5].strip().zfill(2) != "02":
        return None
    cnpj_basico = row[0].strip()
    cnpj_ordem = row[1].strip().zfill(4)
    cnpj_dv = row[2].strip().zfill(2)
    cnpj = build_cnpj(cnpj_basico, cnpj_ordem, cnpj_dv)
    uf = row[19].strip().upper() or None
    if uf is not None and (len(uf) != 2 or not uf.isalpha()):
        uf = None
    return EstablishmentRecord(
        cnpj=cnpj,
        cnpj_basico=cnpj_basico.zfill(8),
        data_abertura=parse_receita_date(row[10]),
        cnae_principal=row[11].strip(),
        uf=uf,
        municipio_codigo=row[20].strip(),
        cnpj_ordem=cnpj_ordem,
        cnpj_dv=cnpj_dv,
        is_matriz=cnpj_ordem == "0001",
    )


def parse_partner_row(row: Sequence[str]) -> PartnerRecord:
    if len(row) < 11:
        raise ValueError("linha de Socios com menos de 11 colunas")
    cnpj_basico = row[0].strip()
    if len(cnpj_basico) != 8 or not cnpj_basico.isdigit():
        raise ValueError("cnpj_basico invalido em Socios")
    partner_name = row[2].strip()
    if not partner_name:
        raise ValueError("nome do socio vazio")
    age_range_code = row[10].strip() or None
    return PartnerRecord(
        cnpj_basico=cnpj_basico,
        partner_identifier=row[1].strip() or None,
        partner_name=partner_name,
        partner_document=row[3].strip() or None,
        partner_qualification_code=row[4].strip() or None,
        entry_date=parse_receita_date(row[5]),
        country_code=row[6].strip() or None,
        legal_representative_document=row[7].strip() or None,
        legal_representative_name=row[8].strip() or None,
        legal_representative_qualification_code=row[9].strip() or None,
        age_range_code=age_range_code,
        age_range=AGE_RANGE_MAP.get(age_range_code),
    )
