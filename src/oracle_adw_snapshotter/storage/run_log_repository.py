from __future__ import annotations

from datetime import datetime, timezone


class RunLogRepository:
    def log_started(self, connection, job_name: str, target_table: str) -> int | None:
        cursor = connection.cursor()
        try:
            run_id_var = cursor.var(int)
            cursor.execute(
                """
                INSERT INTO SNAPSHOT_JOB_RUNS (
                    JOB_NAME,
                    TARGET_TABLE,
                    STARTED_AT_UTC,
                    STATUS
                ) VALUES (
                    :job_name,
                    :target_table,
                    :started_at_utc,
                    :status
                ) RETURNING RUN_ID INTO :run_id
                """,
                job_name=job_name,
                target_table=target_table,
                started_at_utc=datetime.now(timezone.utc),
                status="RUNNING",
                run_id=run_id_var,
            )
            value = run_id_var.getvalue()
            return int(value[0]) if value else None
        except Exception:
            return None
        finally:
            cursor.close()

    def log_finished(self, connection, run_id: int | None, status: str, row_count: int | None = None, error_message: str | None = None) -> None:
        if run_id is None:
            return
        cursor = connection.cursor()
        try:
            cursor.execute(
                """
                UPDATE SNAPSHOT_JOB_RUNS
                SET FINISHED_AT_UTC = :finished_at_utc,
                    STATUS = :status,
                    ROW_COUNT = :row_count,
                    ERROR_MESSAGE = :error_message
                WHERE RUN_ID = :run_id
                """,
                finished_at_utc=datetime.now(timezone.utc),
                status=status,
                row_count=row_count,
                error_message=error_message,
                run_id=run_id,
            )
        except Exception:
            pass
        finally:
            cursor.close()
