from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from html.parser import HTMLParser
from urllib.parse import urljoin

import httpx

from app.models import SourceFile, SourceMonth


REQUIRED_FILE_NAMES = {
    *(f"Empresas{index}.zip" for index in range(10)),
    *(f"Estabelecimentos{index}.zip" for index in range(10)),
    "Naturezas.zip",
    "Municipios.zip",
    "Cnaes.zip",
    "Simples.zip",
}


class NoCompleteSnapshotError(RuntimeError):
    pass


class _LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag.lower() != "a":
            return
        href = dict(attrs).get("href")
        if href:
            self.links.append(href)


class _ApacheIndexParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[list[tuple[str, str | None]]] = []
        self._row: list[tuple[str, str | None]] | None = None
        self._cell_text: list[str] | None = None
        self._cell_href: str | None = None

    def handle_starttag(self, tag: str, attrs) -> None:
        tag = tag.lower()
        if tag == "tr":
            self._row = []
        elif tag in {"td", "th"} and self._row is not None:
            self._cell_text = []
            self._cell_href = None
        elif tag == "a" and self._cell_text is not None:
            self._cell_href = dict(attrs).get("href")

    def handle_data(self, data: str) -> None:
        if self._cell_text is not None:
            self._cell_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"td", "th"} and self._row is not None and self._cell_text is not None:
            text = " ".join("".join(self._cell_text).split())
            self._row.append((text, self._cell_href))
            self._cell_text = None
            self._cell_href = None
        elif tag == "tr" and self._row is not None:
            if self._row:
                self.rows.append(self._row)
            self._row = None


@dataclass(frozen=True)
class SourceManifest:
    source_month: str
    directory_url: str
    files: tuple[SourceFile, ...]


class SourceCatalog:
    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 30,
        client: httpx.Client | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/") + "/"
        self.timeout = timeout
        self.client = client or httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": "cnpj-import-service/1.0"},
        )

    def _links(self, url: str) -> list[str]:
        response = self.client.get(url)
        response.raise_for_status()
        parser = _LinkParser()
        parser.feed(response.text)
        return parser.links

    def _index_text(self, url: str) -> str:
        response = self.client.get(url)
        response.raise_for_status()
        return response.text

    @staticmethod
    def _parse_last_modified(value: str) -> datetime | None:
        value = value.strip()
        if not value or value == "-":
            return None
        try:
            return datetime.strptime(value, "%Y-%m-%d %H:%M")
        except ValueError:
            return None

    def _month_entries(self) -> list[tuple[str, datetime | None]]:
        index_url = urljoin(self.base_url, "arquivos/")
        text = self._index_text(index_url)
        parser = _ApacheIndexParser()
        parser.feed(text)
        entries: dict[str, datetime | None] = {}
        for row in parser.rows:
            href = next(
                (
                    href
                    for _, href in row
                    if href and re.fullmatch(r"\d{4}-\d{2}-\d{2}/?", href)
                ),
                None,
            )
            if href is None:
                continue
            source_month = href.rstrip("/")
            last_modified = (
                self._parse_last_modified(row[2][0]) if len(row) > 2 else None
            )
            entries[source_month] = last_modified
        if not entries:
            link_parser = _LinkParser()
            link_parser.feed(text)
            entries = {
                match.group(1): None
                for link in link_parser.links
                if (match := re.fullmatch(r"(\d{4}-\d{2}-\d{2})/?", link))
            }
        return sorted(entries.items(), key=lambda item: item[0], reverse=True)

    def _available_files(self, source_month: str) -> set[str]:
        directory_url = urljoin(self.base_url, f"arquivos/{source_month}/")
        return {link.rsplit("/", 1)[-1] for link in self._links(directory_url)}

    def list_months(self) -> list[SourceMonth]:
        return [
            SourceMonth(
                source_month=source_month,
                last_modified=last_modified,
                is_complete=REQUIRED_FILE_NAMES <= self._available_files(source_month),
            )
            for source_month, last_modified in self._month_entries()
        ]

    def _files_for_month(self, source_month: str) -> SourceManifest:
        directory_url = urljoin(self.base_url, f"arquivos/{source_month}/")
        available = self._available_files(source_month)
        missing = REQUIRED_FILE_NAMES - available
        if missing:
            missing_text = ", ".join(sorted(missing))
            raise ValueError(f"diretório sem arquivos obrigatórios: {missing_text}")
        files = tuple(
            SourceFile(name=name, url=urljoin(directory_url, name))
            for name in sorted(REQUIRED_FILE_NAMES)
        )
        return SourceManifest(source_month, directory_url, files)

    def resolve_month(self, source_month: str) -> SourceManifest:
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", source_month):
            raise ValueError("source_month deve usar YYYY-MM-DD")
        try:
            datetime.strptime(source_month, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("source_month deve usar uma data YYYY-MM-DD válida") from exc
        return self._files_for_month(source_month)

    def resolve_latest(self) -> SourceManifest:
        month_entries = self._month_entries()
        if not month_entries:
            raise RuntimeError("nenhum diretório mensal encontrado na fonte")
        incomplete_errors: list[str] = []
        for source_month, _ in month_entries:
            try:
                return self.resolve_month(source_month)
            except ValueError as exc:
                incomplete_errors.append(f"{source_month}: {exc}")
        raise NoCompleteSnapshotError(
            "nenhum diretório mensal completo encontrado: " + "; ".join(incomplete_errors[:5])
        )
