from __future__ import annotations

from datetime import datetime, timezone

from oracle_adw_snapshotter.models.types import QueryJob, SnapshotBatch


class QueryService:
    def __init__(self, fetch_size: int = 1000):
        self.fetch_size = fetch_size

    def collect(self, connection, job: QueryJob, target_table: str, query_timeout_seconds: int) -> SnapshotBatch:
        sql = job.resolved_query()
        cursor = connection.cursor()
        try:
            cursor.arraysize = self.fetch_size
            try:
                cursor.execute(f"ALTER SESSION SET SQL_TRACE = FALSE")
            except Exception:
                pass
            try:
                cursor.call_timeout = query_timeout_seconds * 1000
            except Exception:
                pass
            cursor.execute(sql)
            columns = [item[0] for item in cursor.description or []]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            return SnapshotBatch(
                job_name=job.name,
                target_table=target_table,
                columns=columns,
                rows=rows,
                collected_at=datetime.now(timezone.utc),
                source_sql=sql,
            )
        finally:
            cursor.close()
