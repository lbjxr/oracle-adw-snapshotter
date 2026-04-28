from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

from oracle_adw_snapshotter.connectors.oracle import OracleConnector
from oracle_adw_snapshotter.models.types import AppConfig, DatabaseConfig, QueryJob, RandomSchedulerConfig, RuntimeDefaults
from oracle_adw_snapshotter.services.random_schedule import ScheduledRun
from oracle_adw_snapshotter.services.scheduler_loop import RandomSchedulerLoop


@dataclass(slots=True)
class _FakeResult:
    schedule_run_id: int | None = 1
    schedule_name: str = "daily-random-50"
    planned_at_utc: datetime = datetime(2026, 4, 23, 0, 0, tzinfo=timezone.utc)
    started_at_utc: datetime = datetime(2026, 4, 23, 0, 0, tzinfo=timezone.utc)
    finished_at_utc: datetime = datetime(2026, 4, 23, 0, 0, tzinfo=timezone.utc)
    random_value: int = 42
    status: str = "SUCCESS"
    success: bool = True
    summary: str = "ok"
    read_count: int = 1
    wrote_count: int = 1


class _FakePlanner:
    def __init__(self) -> None:
        self._planned = [
            ScheduledRun(
                sequence_no=1,
                planned_at_local=datetime(2026, 4, 23, 8, 0, tzinfo=timezone.utc).astimezone(),
            )
        ]

    def build_day_plan(
        self,
        *,
        run_date: date,
        timezone_name: str,
        runs_per_day: int,
        schedule_name: str = "default",
        seed: int | None = None,
    ):
        return list(self._planned)

    @staticmethod
    def next_wait_seconds(now_local, pending_runs, fallback_seconds: int) -> int:
        return fallback_seconds


class _DisconnectThenSuccessRunner:
    def __init__(self) -> None:
        self.calls = 0

    def execute_once(self, connection, **kwargs):
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("DPY-4011: the database or network closed the connection")
        return _FakeResult()


class _SuccessRunner:
    def __init__(self) -> None:
        self.connection_ids: list[int] = []

    def execute_once(self, connection, **kwargs):
        self.connection_ids.append(connection.connection_id)
        return _FakeResult()


class _FakeConnection:
    def __init__(self, connection_id: int, tracker: list[str]) -> None:
        self.connection_id = connection_id
        self._tracker = tracker
        self.commits = 0

    def commit(self) -> None:
        self.commits += 1
        self._tracker.append(f"commit:{self.connection_id}")

    def close(self) -> None:
        self._tracker.append(f"close:{self.connection_id}")


class _ConnectFactory:
    def __init__(self) -> None:
        self.opened_ids: list[int] = []
        self.closed_ids: list[int] = []
        self.events: list[str] = []

    @contextmanager
    def session(self):
        connection_id = len(self.opened_ids) + 1
        self.opened_ids.append(connection_id)
        self.events.append(f"open:{connection_id}")
        connection = _FakeConnection(connection_id=connection_id, tracker=self.events)
        try:
            yield connection
        finally:
            connection.close()
            self.closed_ids.append(connection_id)


def test_scheduler_reconnects_on_disconnect_error() -> None:
    factory = _ConnectFactory()
    runner = _DisconnectThenSuccessRunner()
    loop = RandomSchedulerLoop(
        planner=_FixedPlanner(),
        execution_runner=runner,
        sleeper=lambda seconds: None,
        reconnect_retry_delay_seconds=1,
        max_reconnect_attempts_per_run=2,
    )

    results = loop.run_forever(connect=factory.session, app_config=_build_app_config(), once=True)

    assert len(results) == 1
    assert runner.calls == 2
    assert factory.opened_ids == [1, 2]
    assert factory.closed_ids == [1, 2]
    assert factory.events == ["open:1", "close:1", "open:2", "commit:2", "close:2"]


def test_scheduler_uses_fresh_connection_per_due_run() -> None:
    factory = _ConnectFactory()
    runner = _SuccessRunner()
    loop = RandomSchedulerLoop(
        planner=_TwoDueRunsPlanner(),
        execution_runner=runner,
        sleeper=lambda seconds: None,
    )

    results = loop.run_forever(connect=factory.session, app_config=_build_app_config(), once=True)

    assert len(results) == 2
    assert runner.connection_ids == [1, 2]
    assert factory.opened_ids == [1, 2]
    assert factory.closed_ids == [1, 2]


def test_is_disconnect_error_detects_dpy_4011() -> None:
    assert OracleConnector.is_disconnect_error(RuntimeError("DPY-4011: the database or network closed the connection")) is True
    assert OracleConnector.is_disconnect_error(RuntimeError("some other failure")) is False


def test_connector_connect_passes_network_resilience_kwargs() -> None:
    connector = OracleConnector(
        DatabaseConfig(
            user="user",
            password="password",
            dsn="dsn",
            connection_mode="thin",
            expire_time_minutes=7,
            retry_count=4,
            retry_delay_seconds=3,
            tcp_connect_timeout_seconds=9.5,
        )
    )

    with patch("oracle_adw_snapshotter.connectors.oracle.oracledb") as fake_oracledb:
        fake_oracledb.connect.return_value = object()
        connection = connector.connect()

    assert connection is fake_oracledb.connect.return_value
    _, kwargs = fake_oracledb.connect.call_args
    assert kwargs["expire_time"] == 7
    assert kwargs["retry_count"] == 4
    assert kwargs["retry_delay"] == 3
    assert kwargs["tcp_connect_timeout"] == 9.5


class _FixedPlanner:
    def build_day_plan(
        self,
        *,
        run_date: date,
        timezone_name: str,
        runs_per_day: int,
        schedule_name: str = "default",
        seed: int | None = None,
    ):
        planned = datetime.now(timezone.utc).astimezone() - timedelta(seconds=1)
        return [ScheduledRun(sequence_no=1, planned_at_local=planned)]

    @staticmethod
    def next_wait_seconds(now_local, pending_runs, fallback_seconds: int) -> int:
        return fallback_seconds


class _TwoDueRunsPlanner:
    def build_day_plan(
        self,
        *,
        run_date: date,
        timezone_name: str,
        runs_per_day: int,
        schedule_name: str = "default",
        seed: int | None = None,
    ):
        planned = datetime.now(timezone.utc).astimezone() - timedelta(seconds=1)
        return [
            ScheduledRun(sequence_no=1, planned_at_local=planned),
            ScheduledRun(sequence_no=2, planned_at_local=planned),
        ]

    @staticmethod
    def next_wait_seconds(now_local, pending_runs, fallback_seconds: int) -> int:
        return fallback_seconds


def _build_app_config() -> AppConfig:
    return AppConfig(
        app_name="oracle-adw-snapshotter",
        db=DatabaseConfig(
            user="user",
            password="password",
            dsn="dsn",
            connection_mode="thin",
        ),
        defaults=RuntimeDefaults(),
        scheduler=RandomSchedulerConfig(
            enabled=True,
            schedule_name="daily-random-50",
            timezone="UTC",
            runs_per_day=1,
            parameter_min=10,
            parameter_max=100,
            read_source_table="SNAPSHOT_SCHEDULE_RUNS",
            read_limit=3,
            poll_interval_seconds=30,
        ),
        jobs=[QueryJob(name="demo_job", source_sql="select 1 from dual")],
        config_path="config/tasks.yaml",
    )
