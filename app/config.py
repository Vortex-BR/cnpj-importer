from __future__ import annotations

from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    database_url: str
    import_api_token: str = Field(min_length=8)
    casa_dos_dados_base_url: str = "https://dados-abertos-rf-cnpj.casadosdados.com.br"
    data_dir: Path = Path("/data")
    categories_config: Path = Path("config/categories.yml")
    batch_size: int = Field(default=5000, ge=100, le=100_000)
    download_timeout: float = Field(default=120, gt=0)
    download_retries: int = Field(default=3, ge=1, le=10)
    max_workers: int = Field(default=2, ge=1, le=10)
    db_pool_max_size: int = Field(default=4, ge=2, le=20)
    worker_poll_interval_seconds: float = Field(default=5, gt=0)
    auto_import_enabled: bool = False
    auto_import_check_interval_seconds: int = Field(default=21600, ge=60)
    auto_import_check_jitter_seconds: int = Field(default=300, ge=0)
    auto_import_max_retries_per_month: int = Field(default=3, ge=0, le=100)
    auto_import_retry_backoff_seconds: int = Field(default=86400, ge=0)
    run_stale_timeout_seconds: int = Field(default=21600, ge=60)
    cache_retention_days: int = Field(default=45, ge=1)
    max_error_samples: int = Field(default=1000, ge=0, le=100_000)
    log_level: str = "INFO"

    @field_validator("log_level")
    @classmethod
    def normalize_log_level(cls, value: str) -> str:
        return value.upper()

    @field_validator("categories_config")
    @classmethod
    def resolve_categories_config(cls, value: Path) -> Path:
        return value.resolve()

    @property
    def cache_dir(self) -> Path:
        return self.data_dir / "cache"
