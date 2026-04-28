from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal


JobMode = Literal["query", "table"]
WriteMode = Literal["append"]
ConnectionMode = Literal["thin", "thick"]


@dataclass(slots=True)
class QueryJob:
    name: str
    enabled: bool = True
    mode: JobMode = "query"
    source_sql: str | None = None
    source_table: str | None = None
    target_table: str | None = None
    where_clause: str | None = None
    write_mode: WriteMode = "append"
    tags: list[str] = field(default_factory=list)

    def resolved_query(self) -> str:
        if self.mode == "query":
            if not self.source_sql:
                raise ValueError(f"Job {self.name} requires source_sql in query mode")
            return self.source_sql.strip().rstrip(";")

        if self.mode == "table":
            if not self.source_table:
                raise ValueError(f"Job {self.name} requires source_table in table mode")
            base_sql = f"SELECT * FROM {self.source_table}"
            if self.where_clause:
                return f"{base_sql} WHERE {self.where_clause}"
            return base_sql

        raise ValueError(f"Unsupported mode: {self.mode}")


@dataclass(slots=True)
class RuntimeDefaults:
    target_schema: str | None = None
    snapshot_table_prefix: str = "SNAP_"
    commit_every_job: bool = True
    query_timeout_seconds: int = 1800


@dataclass(slots=True)
class RandomSchedulerConfig:
    enabled: bool = False
    schedule_name: str = "daily-random-50"
    timezone: str = "Asia/Shanghai"
    runs_per_day: int = 50
    parameter_min: int = 10
    parameter_max: int = 100
    read_source_table: str = "SNAPSHOT_SCHEDULE_RUNS"
    read_limit: int = 3
    poll_interval_seconds: int = 30


@dataclass(slots=True)
class DatabaseConfig:
    user: str
    password: str
    dsn: str
    wallet_dir: str | None = None
    wallet_password: str | None = None
    config_dir: str | None = None
    lib_dir: str | None = None
    connection_mode: ConnectionMode = "thin"
    fetch_size: int = 1000
    expire_time_minutes: int = 5
    retry_count: int = 3
    retry_delay_seconds: int = 2
    tcp_connect_timeout_seconds: float = 15.0


@dataclass(slots=True)
class AppConfig:
    app_name: str
    db: DatabaseConfig
    defaults: RuntimeDefaults
    scheduler: RandomSchedulerConfig
    jobs: list[QueryJob]
    config_path: str


@dataclass(slots=True)
class SnapshotBatch:
    job_name: str
    target_table: str
    columns: list[str]
    rows: list[dict[str, Any]]
    collected_at: datetime
    source_sql: str
