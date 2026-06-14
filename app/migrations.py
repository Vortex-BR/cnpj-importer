from __future__ import annotations

from pathlib import Path
from typing import Protocol


class MigrationConnection(Protocol):
    def execute(self, statement: str): ...


def apply_migrations(
    connection: MigrationConnection,
    migrations_dir: str | Path = "migrations",
) -> None:
    for migration in sorted(Path(migrations_dir).glob("*.sql")):
        connection.execute(migration.read_text(encoding="utf-8"))

