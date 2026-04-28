from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

from oracle_adw_snapshotter.models.types import AppConfig, DatabaseConfig, QueryJob, RandomSchedulerConfig, RuntimeDefaults


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name, default)
    if value == "":
        return default
    return value


def _require_env(name: str) -> str:
    value = _env(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError("Configuration YAML must be a mapping at the top level")
    return data


def load_app_config(config_path: str | None = None, env_file: str | None = None) -> AppConfig:
    if env_file:
        load_dotenv(env_file)
    else:
        load_dotenv()

    resolved_config_path = Path(
        config_path
        or _env("SNAPSHOT_CONFIG_PATH", "config/tasks.yaml")
        or "config/tasks.yaml"
    )
    if not resolved_config_path.is_absolute():
        resolved_config_path = Path.cwd() / resolved_config_path

    yaml_data = _load_yaml(resolved_config_path)
    defaults_raw = yaml_data.get("defaults", {}) or {}
    jobs_raw = yaml_data.get("jobs", []) or []

    defaults = RuntimeDefaults(
        target_schema=defaults_raw.get("target_schema"),
        snapshot_table_prefix=defaults_raw.get("snapshot_table_prefix", "SNAP_"),
        commit_every_job=bool(defaults_raw.get("commit_every_job", True)),
        query_timeout_seconds=int(defaults_raw.get("query_timeout_seconds", 1800)),
    )

    scheduler_raw = yaml_data.get("scheduler", {}) or {}
    scheduler = RandomSchedulerConfig(
        enabled=bool(scheduler_raw.get("enabled", False)),
        schedule_name=str(scheduler_raw.get("schedule_name", "daily-random-50")),
        timezone=str(scheduler_raw.get("timezone", "Asia/Shanghai")),
        runs_per_day=int(scheduler_raw.get("runs_per_day", 50)),
        parameter_min=int(scheduler_raw.get("parameter_min", 10)),
        parameter_max=int(scheduler_raw.get("parameter_max", 100)),
        read_source_table=str(scheduler_raw.get("read_source_table", "SNAPSHOT_SCHEDULE_RUNS")),
        read_limit=int(scheduler_raw.get("read_limit", 3)),
        poll_interval_seconds=int(scheduler_raw.get("poll_interval_seconds", 30)),
    )

    jobs = [
        QueryJob(
            name=job["name"],
            enabled=bool(job.get("enabled", True)),
            mode=job.get("mode", "query"),
            source_sql=job.get("source_sql"),
            source_table=job.get("source_table"),
            target_table=job.get("target_table"),
            where_clause=job.get("where_clause"),
            write_mode=job.get("write_mode", "append"),
            tags=list(job.get("tags", [])),
        )
        for job in jobs_raw
    ]

    db = DatabaseConfig(
        user=_require_env("ORACLE_USER"),
        password=_require_env("ORACLE_PASSWORD"),
        dsn=_require_env("ORACLE_DSN"),
        wallet_dir=_env("ORACLE_WALLET_DIR"),
        wallet_password=_env("ORACLE_WALLET_PASSWORD"),
        config_dir=_env("ORACLE_CONFIG_DIR"),
        lib_dir=_env("ORACLE_LIB_DIR"),
        connection_mode=((_env("ORACLE_CONNECTION_MODE", "thin") or "thin").strip().lower()),
        fetch_size=int(_env("SNAPSHOT_FETCH_SIZE", "1000") or "1000"),
        expire_time_minutes=int(_env("ORACLE_EXPIRE_TIME_MINUTES", "5") or "5"),
        retry_count=int(_env("ORACLE_RETRY_COUNT", "3") or "3"),
        retry_delay_seconds=int(_env("ORACLE_RETRY_DELAY_SECONDS", "2") or "2"),
        tcp_connect_timeout_seconds=float(_env("ORACLE_TCP_CONNECT_TIMEOUT_SECONDS", "15") or "15"),
    )

    return AppConfig(
        app_name=_env("SNAPSHOT_APP_NAME", "oracle-adw-snapshotter") or "oracle-adw-snapshotter",
        db=db,
        defaults=defaults,
        scheduler=scheduler,
        jobs=jobs,
        config_path=str(resolved_config_path),
    )
