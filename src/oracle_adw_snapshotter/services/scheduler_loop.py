from __future__ import annotations

import logging
import time
from dataclasses import asdict
from datetime import datetime
from typing import Callable
from zoneinfo import ZoneInfo

from oracle_adw_snapshotter.connectors.oracle import OracleConnector
from oracle_adw_snapshotter.jobs.random_runner import RandomizedExecutionRunner
from oracle_adw_snapshotter.models.types import AppConfig
from oracle_adw_snapshotter.services.random_schedule import RandomSchedulePlanner


logger = logging.getLogger(__name__)


class RandomSchedulerLoop:
    def __init__(
        self,
        planner: RandomSchedulePlanner | None = None,
        execution_runner: RandomizedExecutionRunner | None = None,
        sleeper=time.sleep,
        reconnect_retry_delay_seconds: int = 5,
        max_reconnect_attempts_per_run: int = 2,
    ):
        self.planner = planner or RandomSchedulePlanner()
        self.execution_runner = execution_runner or RandomizedExecutionRunner()
        self.sleeper = sleeper
        self.reconnect_retry_delay_seconds = max(1, int(reconnect_retry_delay_seconds))
        self.max_reconnect_attempts_per_run = max(1, int(max_reconnect_attempts_per_run))

    def run_forever(
        self,
        connect: Callable[[], object],
        app_config: AppConfig,
        once: bool = False,
    ) -> list[dict]:
        scheduler_config = app_config.scheduler
        tz = ZoneInfo(scheduler_config.timezone)
        completed: list[dict] = []
        current_day = None
        day_plan = []

        while True:
            now_local = datetime.now(tz)
            today = now_local.date()
            if current_day != today:
                current_day = today
                day_plan = self.planner.build_day_plan(
                    run_date=today,
                    timezone_name=scheduler_config.timezone,
                    runs_per_day=scheduler_config.runs_per_day,
                    schedule_name=scheduler_config.schedule_name,
                )

            due_runs = [item for item in day_plan if item.planned_at_local <= now_local]
            pending_runs = [item for item in day_plan if item.planned_at_local > now_local]

            if not due_runs:
                if once:
                    return completed
                wait_seconds = self.planner.next_wait_seconds(
                    now_local=now_local,
                    pending_runs=pending_runs,
                    fallback_seconds=scheduler_config.poll_interval_seconds,
                )
                self.sleeper(wait_seconds)
                continue

            for due in due_runs:
                result = self._execute_due_run(connect=connect, due=due, app_config=app_config)
                completed.append(asdict(result))

            day_plan = pending_runs
            if once:
                return completed

    def _execute_due_run(self, *, connect: Callable[[], object], due, app_config: AppConfig):
        scheduler_config = app_config.scheduler
        last_exc: Exception | None = None

        for attempt in range(1, self.max_reconnect_attempts_per_run + 1):
            try:
                with connect() as connection:
                    result = self.execution_runner.execute_once(
                        connection,
                        schedule_name=scheduler_config.schedule_name,
                        planned_at_utc=due.planned_at_utc,
                        parameter_min=scheduler_config.parameter_min,
                        parameter_max=scheduler_config.parameter_max,
                        read_source_table=scheduler_config.read_source_table,
                        read_limit=scheduler_config.read_limit,
                    )
                    connection.commit()
                    return result
            except Exception as exc:
                last_exc = exc
                if not OracleConnector.is_disconnect_error(exc):
                    raise
                logger.warning(
                    "Scheduler run hit disconnect; will retry with a fresh connection",
                    extra={
                        "attempt": attempt,
                        "max_attempts": self.max_reconnect_attempts_per_run,
                        "schedule_name": scheduler_config.schedule_name,
                        "planned_at_utc": due.planned_at_utc.isoformat(),
                        "error": str(exc),
                    },
                )
                if attempt >= self.max_reconnect_attempts_per_run:
                    raise
                self.sleeper(self.reconnect_retry_delay_seconds)

        assert last_exc is not None
        raise last_exc
