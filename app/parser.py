from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path
from typing import Iterator, Sequence

from app.models import CompanyRecord, EstablishmentRecord
from app.normalization import build_cnpj, parse_capital_social, parse_receita_date


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
    cnpj = build_cnpj(cnpj_basico, row[1], row[2])
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
    )

