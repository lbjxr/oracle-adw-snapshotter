from __future__ import annotations

import json
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from oracle_adw_snapshotter.storage.scheduler_run_repository import SchedulerRunRepository
from oracle_adw_snapshotter.storage.snapshot_reader import SnapshotReader


@dataclass(slots=True)
class RandomRunResult:
    schedule_run_id: int | None
    schedule_name: str
    planned_at_utc: datetime
    started_at_utc: datetime
    finished_at_utc: datetime
    random_value: int
    status: str
    success: bool
    summary: str
    read_count: int
    wrote_count: int


class RandomizedExecutionRunner:
    def __init__(
        self,
        scheduler_run_repository: SchedulerRunRepository | None = None,
        snapshot_reader: SnapshotReader | None = None,
    ):
        self.scheduler_run_repository = scheduler_run_repository or SchedulerRunRepository()
        self.snapshot_reader = snapshot_reader or SnapshotReader()

    def execute_once(
        self,
        connection,
        *,
        schedule_name: str,
        planned_at_utc: datetime,
        parameter_min: int,
        parameter_max: int,
        read_source_table: str,
        read_limit: int,
        seed: int | None = None,
    ) -> RandomRunResult:
        if parameter_min > parameter_max:
            raise ValueError("parameter_min cannot be greater than parameter_max")

        rng = random.Random(seed)
        random_value = rng.randint(parameter_min, parameter_max)
        started_at_utc = datetime.now(timezone.utc)
        schedule_run_id = self.scheduler_run_repository.insert_started(
            connection,
            schedule_name=schedule_name,
            planned_at_utc=planned_at_utc,
            started_at_utc=started_at_utc,
            random_value=random_value,
        )

        try:
            records = self.scheduler_run_repository.safe_fetch_preview(
                connection=connection,
                reader=self.snapshot_reader,
                table_name=read_source_table,
                limit=read_limit,
            )
            read_count = len(records)
            summary = self._build_summary(read_source_table=read_source_table, read_count=read_count, random_value=random_value)
            self.scheduler_run_repository.insert_log_entry(
                connection,
                schedule_run_id=schedule_run_id,
                schedule_name=schedule_name,
                planned_at_utc=planned_at_utc,
                started_at_utc=started_at_utc,
                random_value=random_value,
                read_source_table=read_source_table,
                read_row_count=read_count,
                read_summary=summary,
                payload_json=self._build_payload(records),
            )
            finished_at_utc = datetime.now(timezone.utc)
            self.scheduler_run_repository.mark_finished(
                connection,
                schedule_run_id=schedule_run_id,
                finished_at_utc=finished_at_utc,
                status="SUCCESS",
                success=True,
                summary=summary,
                read_count=read_count,
                wrote_count=1,
            )
            return RandomRunResult(
                schedule_run_id=schedule_run_id,
                schedule_name=schedule_name,
                planned_at_utc=planned_at_utc,
                started_at_utc=started_at_utc,
                finished_at_utc=finished_at_utc,
                random_value=random_value,
                status="SUCCESS",
                success=True,
                summary=summary,
                read_count=read_count,
                wrote_count=1,
            )
        except Exception as exc:
            finished_at_utc = datetime.now(timezone.utc)
            self.scheduler_run_repository.mark_finished(
                connection,
                schedule_run_id=schedule_run_id,
                finished_at_utc=finished_at_utc,
                status="FAILED",
                success=False,
                summary=str(exc),
                error_message=str(exc),
            )
            raise

    @staticmethod
    def _build_summary(*, read_source_table: str, read_count: int, random_value: int) -> str:
        return (
            f"Read {read_count} row(s) from {read_source_table.upper()} and wrote scheduler log "
            f"with random_value={random_value}"
        )

    @staticmethod
    def _build_payload(records: list[Any]) -> str:
        preview: list[dict[str, Any]] = []
        for record in records:
            if hasattr(record, "to_dict"):
                preview.append(record.to_dict())
            elif isinstance(record, dict):
                preview.append(record)
            else:
                preview.append({"value": str(record)})
        return json.dumps({"preview": preview}, ensure_ascii=False, default=str)
