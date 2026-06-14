from __future__ import annotations

import argparse
import json

from app.config import Settings
from app.logging_config import configure_logging
from app.maintenance import cleanup_cache
from app.migrations import apply_migrations
from app.runtime import build_runtime


def _json(value) -> str:
    return json.dumps(value, default=str, ensure_ascii=False, indent=2)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Importador de CNPJs")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("import-latest")
    import_month = subcommands.add_parser("import-month")
    import_month.add_argument("source_month")
    subcommands.add_parser("stats")
    subcommands.add_parser("cleanup-cache")
    args = parser.parse_args(argv)

    settings = Settings()
    configure_logging(settings.log_level)
    runtime = build_runtime(settings)
    runtime.database.open()
    try:
        with runtime.database.connection() as connection:
            apply_migrations(connection)
        if args.command == "import-latest":
            run_id = runtime.import_service.enqueue_latest()
            if not runtime.import_service.run_import(run_id):
                raise RuntimeError("outra importação está em execução")
            print(_json({"run_id": run_id, "status": "SUCCEEDED"}))
        elif args.command == "import-month":
            run_id = runtime.import_service.enqueue_month(args.source_month)
            if not runtime.import_service.run_import(run_id):
                raise RuntimeError("outra importação está em execução")
            print(_json({"run_id": run_id, "status": "SUCCEEDED"}))
        elif args.command == "stats":
            with runtime.database.connection() as connection:
                print(_json(runtime.repository.stats(connection)))
        elif args.command == "cleanup-cache":
            with runtime.database.connection() as connection:
                protected = runtime.repository.get_active_source_months(connection)
            result = cleanup_cache(
                settings.cache_dir,
                protected_months=protected,
                older_than_days=settings.cache_retention_days,
            )
            print(_json(result.__dict__))
    finally:
        runtime.database.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
