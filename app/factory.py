from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import httpx
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.responses import JSONResponse

from app.config import Settings
from app.importer import ADVISORY_LOCK_ID
from app.maintenance import cleanup_cache
from app.migrations import apply_migrations
from app.repository import ActiveRunConflict
from app.runtime import build_runtime
from app.security import require_import_token


def create_app(
    *,
    settings: Settings,
    database=None,
    repository=None,
    import_service=None,
    worker=None,
    scheduler=None,
    run_startup_migrations: bool = True,
) -> FastAPI:
    if any(
        item is None
        for item in (database, repository, import_service, worker, scheduler)
    ):
        runtime = build_runtime(settings)
        database = database or runtime.database
        repository = repository or runtime.repository
        import_service = import_service or runtime.import_service
        worker = worker or runtime.worker
        scheduler = scheduler or runtime.scheduler

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        settings.cache_dir.mkdir(parents=True, exist_ok=True)
        database.open()
        if run_startup_migrations:
            with database.connection() as connection:
                apply_migrations(connection)
                repository.recover_stale_runs(
                    connection,
                    timeout_seconds=settings.run_stale_timeout_seconds,
                )
                repository.recover_orphaned_runs(
                    connection,
                    lock_id=ADVISORY_LOCK_ID,
                )
        worker.start()
        scheduler.start()
        try:
            yield
        finally:
            scheduler.stop()
            worker.stop()
            database.close()

    app = FastAPI(
        title="CNPJ Import Service",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.database = database
    app.state.repository = repository
    app.state.import_service = import_service
    app.state.worker = worker
    app.state.scheduler = scheduler

    protected = [Depends(require_import_token)]

    @app.get("/health")
    def health(request: Request, response: Response):
        database_ok = request.app.state.database.check()
        worker_state = request.app.state.worker
        scheduler_state = request.app.state.scheduler
        auto_state = None
        if database_ok:
            with request.app.state.database.connection() as connection:
                auto_state = request.app.state.repository.get_auto_import_state(
                    connection
                )
        auto_state = auto_state or {}
        if not database_ok:
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {
            "status": "ok" if database_ok else "unavailable",
            "database": "ok" if database_ok else "error",
            "worker": "running" if worker_state.running else "stopped",
            "worker_last_error": worker_state.last_error,
            "auto_import_enabled": request.app.state.settings.auto_import_enabled,
            "auto_scheduler": (
                "running"
                if scheduler_state.running
                else (
                    "disabled"
                    if not request.app.state.settings.auto_import_enabled
                    else "stopped"
                )
            ),
            "auto_scheduler_last_error": scheduler_state.last_error,
            "last_auto_check_at": auto_state.get("last_auto_check_at"),
            "last_auto_check_result": auto_state.get("last_auto_check_result"),
            "next_auto_check_at": auto_state.get("next_auto_check_at"),
            "last_detected_source_month": auto_state.get(
                "last_detected_source_month"
            ),
        }

    @app.get("/sources/months", dependencies=protected)
    async def source_months(request: Request):
        try:
            items = await asyncio.to_thread(
                request.app.state.import_service.list_source_months
            )
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        return {"items": items}

    def active_run_conflict(exc: ActiveRunConflict) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "detail": "já existe uma importação ativa",
                "active_run": {
                    "run_id": exc.active_run["id"],
                    "source_month": exc.active_run["source_month"],
                    "status": exc.active_run["status"],
                },
            },
        )

    @app.get("/stats", dependencies=protected)
    def stats_endpoint(request: Request):
        with request.app.state.database.connection() as connection:
            return request.app.state.repository.stats(connection)

    @app.post(
        "/imports/latest",
        status_code=status.HTTP_202_ACCEPTED,
        dependencies=protected,
    )
    async def import_latest(request: Request):
        try:
            run_id = await asyncio.to_thread(
                request.app.state.import_service.enqueue_latest
            )
        except ActiveRunConflict as exc:
            return active_run_conflict(exc)
        except (ValueError, httpx.HTTPError, RuntimeError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        return {"run_id": run_id, "status": "QUEUED"}

    @app.post(
        "/imports/month/{source_month}",
        status_code=status.HTTP_202_ACCEPTED,
        dependencies=protected,
    )
    async def import_month(source_month: str, request: Request):
        try:
            run_id = await asyncio.to_thread(
                request.app.state.import_service.enqueue_month,
                source_month,
            )
        except ActiveRunConflict as exc:
            return active_run_conflict(exc)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        return {"run_id": run_id, "status": "QUEUED"}

    @app.get("/imports/runs", dependencies=protected)
    def import_runs(
        request: Request,
        limit: int = Query(default=20, ge=1, le=100),
        offset: int = Query(default=0, ge=0),
    ):
        with request.app.state.database.connection() as connection:
            return {
                "items": request.app.state.repository.list_runs(
                    connection,
                    limit=limit,
                    offset=offset,
                ),
                "limit": limit,
                "offset": offset,
            }

    @app.get("/imports/runs/{run_id}", dependencies=protected)
    def import_run(run_id: int, request: Request):
        with request.app.state.database.connection() as connection:
            run = request.app.state.repository.get_run(connection, run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="importação não encontrada")
        return run

    @app.post("/maintenance/cleanup-cache", dependencies=protected)
    async def cleanup_cache_endpoint(request: Request):
        with request.app.state.database.connection() as connection:
            protected_months = request.app.state.repository.get_active_source_months(
                connection
            )
        result = await asyncio.to_thread(
            cleanup_cache,
            request.app.state.settings.cache_dir,
            protected_months=protected_months,
            older_than_days=request.app.state.settings.cache_retention_days,
        )
        return {
            "removed_months": result.removed_months,
            "freed_bytes": result.freed_bytes,
        }

    return app
