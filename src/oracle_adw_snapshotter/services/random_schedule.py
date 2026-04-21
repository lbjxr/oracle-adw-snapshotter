from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo


@dataclass(slots=True, frozen=True)
class ScheduledRun:
    sequence_no: int
    planned_at_local: datetime

    @property
    def planned_at_utc(self) -> datetime:
        return self.planned_at_local.astimezone(ZoneInfo("UTC"))


class RandomSchedulePlanner:
    def build_day_plan(
        self,
        *,
        run_date: date,
        timezone_name: str,
        runs_per_day: int,
        seed: int | None = None,
    ) -> list[ScheduledRun]:
        if runs_per_day <= 0:
            raise ValueError("runs_per_day must be greater than 0")
        if runs_per_day > 86400:
            raise ValueError("runs_per_day cannot exceed seconds in a day")

        tz = ZoneInfo(timezone_name)
        rng = random.Random(seed)
        second_offsets = sorted(rng.sample(range(24 * 60 * 60), runs_per_day))
        start_of_day = datetime.combine(run_date, time.min, tzinfo=tz)
        return [
            ScheduledRun(
                sequence_no=index,
                planned_at_local=start_of_day + timedelta(seconds=offset),
            )
            for index, offset in enumerate(second_offsets, start=1)
        ]

    @staticmethod
    def next_wait_seconds(now_local: datetime, pending_runs: list[ScheduledRun], fallback_seconds: int) -> int:
        for item in pending_runs:
            if item.planned_at_local > now_local:
                delta = int((item.planned_at_local - now_local).total_seconds())
                return max(1, min(delta, fallback_seconds))
        return max(1, fallback_seconds)
