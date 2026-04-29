from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from oracle_adw_snapshotter.jobs.random_runner import RandomizedExecutionRunner
from oracle_adw_snapshotter.models.types import AppConfig, DatabaseConfig, QueryJob, RandomSchedulerConfig, RuntimeDefaults
from oracle_adw_snapshotter.services.random_schedule import ScheduledRun
from oracle_adw_snapshotter.services.scheduler_loop import RandomSchedulerLoop
from oracle_adw_snapshotter.storage.scheduler_run_repository import ScheduleRunClaim


@dataclass(slots=True)
class _StoredRun:
    schedule_run_id: int
    schedule_name: str
    planned_at_utc: datetime
    status: str
    summary: str | None = None
    success: bool | None = None
    started_at_utc: datetime | None = None
    finished_at_utc: datetime | None = None
    random_value: int | None = None
    read_count: int | None = None
    wrote_count: int | None = None
    error_message: str | None = None


class _InMemorySchedulerRunRepository:
    def __init__(self) -> None:
        self.runs: dict[tuple[str, datetime], _StoredRun] = {}
        self.logs: list[dict] = []
        self.next_id = 1

    def claim_scheduled_run(self, connection, *, schedule_name: str, planned_at_utc: datetime, started_at_utc: datetime, random_value: int):
        key = (schedule_name, planned_at_utc)
        existing = self.runs.get(key)
        if existing is not None:
            if existing.status in {"RUNNING", "SUCCESS"}:
                return ScheduleRunClaim(
                    schedule_run_id=existing.schedule_run_id,
                    status=existing.status,
                    already_processed=True,
                    already_finished=(existing.status == "SUCCESS"),
                    summary=existing.summary,
                )
            existing.status = "RUNNING"
            existing.summary = None
            existing.success = None
            existing.started_at_utc = started_at_utc
            existing.finished_at_utc = None
            existing.random_value = random_value
            existing.read_count = None
            existing.wrote_count = None
            existing.error_message = None
            return ScheduleRunClaim(
                schedule_run_id=existing.schedule_run_id,
                status="RUNNING",
                already_processed=False,
                already_finished=False,
            )

        run = _StoredRun(
            schedule_run_id=self.next_id,
            schedule_name=schedule_name,
            planned_at_utc=planned_at_utc,
            status="RUNNING",
            started_at_utc=started_at_utc,
            random_value=random_value,
        )
        self.next_id += 1
        self.runs[key] = run
        return ScheduleRunClaim(
            schedule_run_id=run.schedule_run_id,
            status=run.status,
            already_processed=False,
            already_finished=False,
        )

    def list_processed_planned_slots(self, connection, *, schedule_name: str, planned_at_utc_values: list[datetime]) -> set[datetime]:
        processed: set[datetime] = set()
        for planned_at_utc in planned_at_utc_values:
            existing = self.runs.get((schedule_name, planned_at_utc))
            if existing and existing.status in {"RUNNING", "SUCCESS"}:
                processed.add(planned_at_utc)
        return processed

    def safe_fetch_preview(self, connection, reader, table_name: str, limit: int):
        return [{"id": idx} for idx in range(limit)]

    def insert_log_entry(self, connection, **kwargs) -> None:
        self.logs.append(kwargs)

    def mark_finished(self, connection, *, schedule_run_id: int | None, finished_at_utc: datetime, status: str, success: bool, summary: str, read_count: int | None = None, wrote_count: int | None = None, error_message: str | None = None) -> None:
        for run in self.runs.values():
            if run.schedule_run_id == schedule_run_id:
                run.finished_at_utc = finished_at_utc
                run.status = status
                run.success = success
                run.summary = summary
                run.read_count = read_count
                run.wrote_count = wrote_count
                run.error_message = error_message
                return
        raise AssertionError(f"Unknown schedule_run_id: {schedule_run_id}")


class _PlannerWithSingleDueRun:
    def __init__(self, planned_at_local: datetime) -> None:
        self.planned_at_local = planned_at_local

    def build_day_plan(self, *, run_date: date, timezone_name: str, runs_per_day: int, schedule_name: str = "default", seed: int | None = None):
        return [ScheduledRun(sequence_no=1, planned_at_local=self.planned_at_local)]

    @staticmethod
    def next_wait_seconds(now_local, pending_runs, fallback_seconds: int) -> int:
        return fallback_seconds


class _FakeConnection:
    def __init__(self) -> None:
        self.commits = 0

    def commit(self) -> None:
        self.commits += 1

    def close(self) -> None:
        return None


class _ConnectFactory:
    def __init__(self) -> None:
        self.connections: list[_FakeConnection] = []

    @contextmanager
    def session(self):
        connection = _FakeConnection()
        self.connections.append(connection)
        yield connection


def test_same_planned_slot_is_executed_only_once() -> None:
    repo = _InMemorySchedulerRunRepository()
    runner = RandomizedExecutionRunner(scheduler_run_repository=repo)
    planned_at_utc = datetime(2026, 4, 29, 12, 0, tzinfo=timezone.utc)

    first = runner.execute_once(
        object(),
        schedule_name="daily-random-50",
        planned_at_utc=planned_at_utc,
        parameter_min=10,
        parameter_max=10,
        read_source_table="SNAPSHOT_SCHEDULE_RUNS",
        read_limit=2,
        seed=1,
    )
    second = runner.execute_once(
        object(),
        schedule_name="daily-random-50",
        planned_at_utc=planned_at_utc,
        parameter_min=10,
        parameter_max=10,
        read_source_table="SNAPSHOT_SCHEDULE_RUNS",
        read_limit=2,
        seed=1,
    )

    assert first.skipped is False
    assert first.status == "SUCCESS"
    assert second.skipped is True
    assert second.status == "SUCCESS"
    assert len(repo.runs) == 1
    assert len(repo.logs) == 1


def test_scheduler_once_does_not_replay_already_finished_due_runs() -> None:
    repo = _InMemorySchedulerRunRepository()
    runner = RandomizedExecutionRunner(scheduler_run_repository=repo)
    loop = RandomSchedulerLoop(execution_runner=runner, planner=_PlannerWithSingleDueRun(datetime.now(timezone.utc) - timedelta(minutes=1)), sleeper=lambda seconds: None)
    factory = _ConnectFactory()
    app_config = _build_app_config()

    first = loop.run_forever(connect=factory.session, app_config=app_config, once=True)
    second = loop.run_forever(connect=factory.session, app_config=app_config, once=True)

    assert len(first) == 1
    assert first[0]["skipped"] is False
    assert second == []
    assert len(repo.logs) == 1
    assert len(repo.runs) == 1


def test_loop_filters_already_processed_slots_when_day_plan_is_rebuilt() -> None:
    repo = _InMemorySchedulerRunRepository()
    planned_at_local = datetime.now(timezone.utc) - timedelta(minutes=1)
    planned_at_utc = planned_at_local.astimezone(timezone.utc)
    repo.runs[("daily-random-50", planned_at_utc)] = _StoredRun(
        schedule_run_id=1,
        schedule_name="daily-random-50",
        planned_at_utc=planned_at_utc,
        status="SUCCESS",
        summary="already done",
        success=True,
    )

    runner = RandomizedExecutionRunner(scheduler_run_repository=repo)
    loop = RandomSchedulerLoop(execution_runner=runner, planner=_PlannerWithSingleDueRun(planned_at_local), sleeper=lambda seconds: None)
    factory = _ConnectFactory()

    results = loop.run_forever(connect=factory.session, app_config=_build_app_config(), once=True)

    assert results == []
    assert len(repo.logs) == 0
    assert len(repo.runs) == 1


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
