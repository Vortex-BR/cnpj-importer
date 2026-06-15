from __future__ import annotations

import re
import unicodedata
from datetime import datetime, date
from decimal import Decimal, InvalidOperation

from app.models import Porte


PORTE_MAP = {
    "": Porte("NÃO INFORMADO", "NAO_INFORMADO"),
    "00": Porte("NÃO INFORMADO", "NAO_INFORMADO"),
    "01": Porte("MICRO EMPRESA", "ME"),
    "03": Porte("EMPRESA DE PEQUENO PORTE", "EPP"),
    "05": Porte("DEMAIS", "DEMAIS"),
}

AGE_RANGE_MAP = {
    "0": "NAO SE APLICA",
    "1": "0 A 12 ANOS",
    "2": "13 A 20 ANOS",
    "3": "21 A 30 ANOS",
    "4": "31 A 40 ANOS",
    "5": "41 A 50 ANOS",
    "6": "51 A 60 ANOS",
    "7": "61 A 70 ANOS",
    "8": "71 A 80 ANOS",
    "9": "MAIOR DE 80 ANOS",
}


def strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    return "".join(char for char in normalized if not unicodedata.combining(char))


def normalize_porte(code: str | None, *, is_mei: bool = False) -> Porte:
    normalized_code = (code or "").strip().zfill(2) if (code or "").strip() else ""
    porte = PORTE_MAP.get(normalized_code, PORTE_MAP[""])
    if is_mei:
        return Porte(porte.description, "MEI")
    return porte


def normalize_nature_group(description: str | None) -> str:
    text = re.sub(r"\s+", " ", strip_accents(description or "").upper()).strip()
    if "UNIPESSOAL" in text:
        return "SLU"
    if "LIMITADA" in text or re.search(r"\bLTDA\b", text):
        return "LTDA"
    if "EMPRESARIO" in text:
        return "EI"
    if "SOCIEDADE ANONIMA" in text or re.search(r"\bS\s*/?\s*A\b", text):
        return "SA"
    if "ASSOCIACAO" in text:
        return "ASSOCIACAO"
    if "COOPERATIVA" in text:
        return "COOPERATIVA"
    return "OUTROS"


def _digits(value: str, length: int, field_name: str) -> str:
    value = (value or "").strip()
    if not value.isdigit() or len(value) > length:
        raise ValueError(f"{field_name} inválido")
    return value.zfill(length)


def build_cnpj(cnpj_basico: str, cnpj_ordem: str, cnpj_dv: str) -> str:
    return "".join(
        (
            _digits(cnpj_basico, 8, "cnpj_basico"),
            _digits(cnpj_ordem, 4, "cnpj_ordem"),
            _digits(cnpj_dv, 2, "cnpj_dv"),
        )
    )


def parse_receita_date(raw: str | None) -> date | None:
    value = (raw or "").strip()
    if not value or value == "00000000":
        return None
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError:
        return None


def parse_capital_social(raw: str | None) -> Decimal | None:
    value = (raw or "").strip().replace(".", "").replace(",", ".")
    if not value:
        return None
    try:
        return Decimal(value)
    except InvalidOperation:
        return None
