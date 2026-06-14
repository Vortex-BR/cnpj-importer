from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path

import httpx

from app.models import SourceFile


@dataclass(frozen=True)
class DownloadResult:
    path: Path
    etag: str | None
    content_length: int | None
    cached: bool


class ResumableDownloader:
    def __init__(
        self,
        cache_dir: str | Path,
        *,
        timeout: float,
        retries: int = 3,
        client: httpx.Client | None = None,
    ) -> None:
        self.cache_dir = Path(cache_dir)
        self.timeout = timeout
        self.retries = retries
        self.client = client or httpx.Client(
            timeout=httpx.Timeout(timeout, connect=min(timeout, 30)),
            follow_redirects=True,
            headers={"User-Agent": "cnpj-import-service/1.0"},
        )

    @staticmethod
    def _load_metadata(path: Path) -> dict:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return {}

    @staticmethod
    def _write_metadata(path: Path, payload: dict) -> None:
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
        os.replace(temporary, path)

    def _remote_metadata(self, source_file: SourceFile) -> tuple[int | None, str | None]:
        content_length = source_file.content_length
        etag = source_file.etag
        try:
            response = self.client.head(source_file.url)
            if response.status_code < 400:
                content_length_header = response.headers.get("Content-Length")
                if content_length_header and content_length_header.isdigit():
                    content_length = int(content_length_header)
                etag = response.headers.get("ETag") or etag
        except httpx.HTTPError:
            if content_length is None and etag is None:
                raise
        return content_length, etag

    def download(self, source_month: str, source_file: SourceFile) -> DownloadResult:
        month_dir = self.cache_dir / source_month
        month_dir.mkdir(parents=True, exist_ok=True)
        final_path = month_dir / source_file.name
        partial_path = month_dir / f"{source_file.name}.part"
        metadata_path = month_dir / f"{source_file.name}.meta.json"

        content_length, etag = self._remote_metadata(source_file)
        metadata = self._load_metadata(metadata_path)
        metadata_matches = (
            (etag is None or metadata.get("etag") == etag)
            and (
                content_length is None
                or metadata.get("content_length") == content_length
            )
        )
        if (
            final_path.exists()
            and metadata_matches
            and (content_length is None or final_path.stat().st_size == content_length)
        ):
            return DownloadResult(final_path, etag, content_length, True)

        if not metadata_matches:
            partial_path.unlink(missing_ok=True)
            final_path.unlink(missing_ok=True)

        self._write_metadata(
            metadata_path,
            {"etag": etag, "content_length": content_length},
        )
        if (
            partial_path.exists()
            and content_length is not None
            and partial_path.stat().st_size == content_length
        ):
            os.replace(partial_path, final_path)
            return DownloadResult(final_path, etag, content_length, True)

        for attempt in range(1, self.retries + 1):
            try:
                offset = partial_path.stat().st_size if partial_path.exists() else 0
                headers = {"Range": f"bytes={offset}-"} if offset else {}
                with self.client.stream("GET", source_file.url, headers=headers) as response:
                    response.raise_for_status()
                    append = bool(offset and response.status_code == 206)
                    mode = "ab" if append else "wb"
                    with partial_path.open(mode) as destination:
                        for chunk in response.iter_bytes():
                            destination.write(chunk)
                if content_length is not None and partial_path.stat().st_size != content_length:
                    raise IOError(
                        f"tamanho inválido para {source_file.name}: "
                        f"{partial_path.stat().st_size}/{content_length}"
                    )
                os.replace(partial_path, final_path)
                return DownloadResult(final_path, etag, content_length, False)
            except (httpx.HTTPError, OSError):
                if attempt >= self.retries:
                    raise
                time.sleep(min(2 ** (attempt - 1), 10))
        raise RuntimeError("download não concluído")
