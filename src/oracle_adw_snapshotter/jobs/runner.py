from __future__ import annotations

from dataclasses import dataclass

from oracle_adw_snapshotter.models.types import AppConfig, QueryJob
from oracle_adw_snapshotter.services.query_service import QueryService
from oracle_adw_snapshotter.storage.run_log_repository import RunLogRepository
from oracle_adw_snapshotter.storage.snapshot_writer import SnapshotWriter


@dataclass(slots=True)
class JobResult:
    job_name: str
    target_table: str
    collected_rows: int


class SnapshotJobRunner:
    def __init__(
        self,
        query_service: QueryService,
        snapshot_writer: SnapshotWriter,
        run_log_repository: RunLogRepository | None = None,
    ):
        self.query_service = query_service
        self.snapshot_writer = snapshot_writer
        self.run_log_repository = run_log_repository or RunLogRepository()

    def run_jobs(self, connection, app_config: AppConfig, job_names: list[str] | None = None) -> list[JobResult]:
        selected = [job for job in app_config.jobs if job.enabled]
        if job_names:
            requested = set(job_names)
            selected = [job for job in selected if job.name in requested]

        results: list[JobResult] = []
        for job in selected:
            results.append(self.run_job(connection, job, app_config))
            if app_config.defaults.commit_every_job:
                connection.commit()

        return results

    def run_job(self, connection, job: QueryJob, app_config: AppConfig) -> JobResult:
        target_table = self._resolve_target_table(job, app_config)
        run_id = self.run_log_repository.log_started(connection, job.name, target_table)
        try:
            batch = self.query_service.collect(
                connection=connection,
                job=job,
                target_table=target_table,
                query_timeout_seconds=app_config.defaults.query_timeout_seconds,
            )
            written = self.snapshot_writer.write_batch(connection, batch)
            self.run_log_repository.log_finished(connection, run_id, status="SUCCESS", row_count=written)
            return JobResult(job_name=job.name, target_table=target_table, collected_rows=written)
        except Exception as exc:
            self.run_log_repository.log_finished(connection, run_id, status="FAILED", error_message=str(exc))
            raise

    @staticmethod
    def _resolve_target_table(job: QueryJob, app_config: AppConfig) -> str:
        if job.target_table:
            return job.target_table
        normalized = job.name.upper().replace("-", "_")
        prefix = app_config.defaults.snapshot_table_prefix
        return f"{prefix}{normalized}"
