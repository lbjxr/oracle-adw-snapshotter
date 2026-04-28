from __future__ import annotations

from datetime import date

from oracle_adw_snapshotter.services.random_schedule import RandomSchedulePlanner


def _offsets(plan):
    return [item.planned_at_local.time().isoformat() for item in plan]


def test_day_plan_is_stable_for_same_day_and_schedule_inputs() -> None:
    planner = RandomSchedulePlanner()

    first = planner.build_day_plan(
        run_date=date(2026, 4, 28),
        timezone_name="Asia/Shanghai",
        runs_per_day=5,
        schedule_name="daily-random-50",
    )
    second = planner.build_day_plan(
        run_date=date(2026, 4, 28),
        timezone_name="Asia/Shanghai",
        runs_per_day=5,
        schedule_name="daily-random-50",
    )

    assert _offsets(first) == _offsets(second)


def test_day_plan_can_change_on_different_day() -> None:
    planner = RandomSchedulePlanner()

    today_plan = planner.build_day_plan(
        run_date=date(2026, 4, 28),
        timezone_name="Asia/Shanghai",
        runs_per_day=5,
        schedule_name="daily-random-50",
    )
    next_day_plan = planner.build_day_plan(
        run_date=date(2026, 4, 29),
        timezone_name="Asia/Shanghai",
        runs_per_day=5,
        schedule_name="daily-random-50",
    )

    assert _offsets(today_plan) != _offsets(next_day_plan)


def test_day_plan_changes_when_schedule_name_changes() -> None:
    planner = RandomSchedulePlanner()

    alpha_plan = planner.build_day_plan(
        run_date=date(2026, 4, 28),
        timezone_name="Asia/Shanghai",
        runs_per_day=5,
        schedule_name="daily-random-50",
    )
    beta_plan = planner.build_day_plan(
        run_date=date(2026, 4, 28),
        timezone_name="Asia/Shanghai",
        runs_per_day=5,
        schedule_name="daily-random-51",
    )

    assert _offsets(alpha_plan) != _offsets(beta_plan)


def test_day_plan_changes_when_timezone_changes() -> None:
    planner = RandomSchedulePlanner()

    shanghai_plan = planner.build_day_plan(
        run_date=date(2026, 4, 28),
        timezone_name="Asia/Shanghai",
        runs_per_day=5,
        schedule_name="daily-random-50",
    )
    utc_plan = planner.build_day_plan(
        run_date=date(2026, 4, 28),
        timezone_name="UTC",
        runs_per_day=5,
        schedule_name="daily-random-50",
    )

    assert _offsets(shanghai_plan) != _offsets(utc_plan)
