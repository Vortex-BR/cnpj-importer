from __future__ import annotations

import logging
import random
import threading
from datetime import datetime, timedelta
from typing import Callable


LOGGER = logging.getLogger(__name__)


class AutoImportScheduler:
    def __init__(
        self,
        import_service,
        *,
        enabled: bool,
        check_interval_seconds: int,
        jitter_seconds: int,
        max_retries: int,
        retry_backoff_seconds: int,
        random_int: Callable[[int, int], int] = random.randint,
        clock: Callable[[], datetime] = datetime.utcnow,
    ) -> None:
        self.import_service = import_service
        self.enabled = enabled
        self.check_interval_seconds = check_interval_seconds
        self.jitter_seconds = jitter_seconds
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self.random_int = random_int
        self.clock = clock
        self.last_error: str | None = None
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._initial_delay_seconds: int | None = None

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def next_delay_seconds(self) -> int:
        jitter = (
            self.random_int(0, self.jitter_seconds)
            if self.jitter_seconds > 0
            else 0
        )
        return self.check_interval_seconds + jitter

    def start(self) -> None:
        if not self.enabled:
            self.import_service.update_auto_import_state(
                checked_at=None,
                result="DISABLED",
                next_check_at=None,
                source_month=None,
            )
            return
        if self.running:
            return
        self._stop_event.clear()
        delay = self.next_delay_seconds()
        self._initial_delay_seconds = delay
        self.import_service.update_auto_import_state(
            checked_at=None,
            result=None,
            next_check_at=self.clock() + timedelta(seconds=delay),
            source_month=None,
        )
        self._thread = threading.Thread(
            target=self._run,
            name="cnpj-auto-import-scheduler",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=10)
        self._thread = None

    def check_once(self) -> int:
        checked_at = self.clock()
        try:
            result = self.import_service.check_auto_import_once(
                max_retries=self.max_retries,
                retry_backoff_seconds=self.retry_backoff_seconds,
            )
            self.last_error = None
            result_name = result.result
            source_month = result.source_month
            if result_name == "ENQUEUED":
                LOGGER.info(
                    "Auto-import enfileirou o snapshot %s como run %s",
                    source_month,
                    result.run_id,
                )
            else:
                LOGGER.info(
                    "Auto-import sem novo job: result=%s source_month=%s",
                    result_name,
                    source_month,
                )
        except Exception as exc:
            self.last_error = str(exc)
            result_name = "CHECK_FAILED"
            source_month = None
            LOGGER.exception("Falha na verificação automática de snapshots")
        next_delay = self.next_delay_seconds()
        next_check_at = checked_at + timedelta(seconds=next_delay)
        self.import_service.update_auto_import_state(
            checked_at=checked_at,
            result=result_name,
            next_check_at=next_check_at,
            source_month=source_month,
        )
        return next_delay

    def _run(self) -> None:
        delay = self._initial_delay_seconds or self.next_delay_seconds()
        while not self._stop_event.wait(delay):
            delay = self.check_once()
