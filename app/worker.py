from __future__ import annotations

import logging
import threading


LOGGER = logging.getLogger(__name__)


class RunHeartbeat:
    def __init__(
        self,
        database,
        repository,
        *,
        run_id: int,
        interval_seconds: float,
    ) -> None:
        self.database = database
        self.repository = repository
        self.run_id = run_id
        self.interval_seconds = interval_seconds
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        if self.running:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name=f"cnpj-run-heartbeat-{self.run_id}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1, self.interval_seconds + 1))
        self._thread = None

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                with self.database.connection() as connection:
                    self.repository.heartbeat_run(
                        connection,
                        run_id=self.run_id,
                    )
            except Exception:
                LOGGER.exception(
                    "Falha ao atualizar heartbeat da importação %s",
                    self.run_id,
                )
            self._stop_event.wait(self.interval_seconds)


class ImportWorker:
    def __init__(self, import_service, *, poll_interval: float) -> None:
        self.import_service = import_service
        self.poll_interval = poll_interval
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self.last_error: str | None = None

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        if self.running:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="cnpj-import-worker",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=10)
        self._thread = None

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                processed = self.import_service.process_queued_once()
                self.last_error = None
            except Exception as exc:
                self.last_error = str(exc)
                LOGGER.exception("Falha no worker de importação")
                processed = False
            if not processed:
                self._stop_event.wait(self.poll_interval)
