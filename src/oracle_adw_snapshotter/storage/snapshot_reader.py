from __future__ import annotations

import csv
import io
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from oracle_adw_snapshotter.models.types import AppConfig

_IDENTIFIER_RE = re.compile(r"^[A-Z][A-Z0-9_$#]*$")


@dataclass(slots=True)
class SnapshotRecord:
    snapshot_id: int | None
    job_name: str | None
    collected_at_utc: datetime | None
    source_sql: str | None
    payload_json: Any
    extra_fields: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "SNAPSHOT_ID": self.snapshot_id,
            "JOB_NAME": self.job_name,
            "COLLECTED_AT_UTC": self.collected_at_utc.isoformat() if self.collected_at_utc else None,
            "SOURCE_SQL": self.source_sql,
            "PAYLOAD_JSON": self.payload_json,
        }
        if self.extra_fields:
            return dict(self.extra_fields)
        return payload


class SnapshotReader:
    def resolve_table_for_job(self, app_config: AppConfig, job_name: str) -> str:
        for job in app_config.jobs:
            if job.name == job_name:
                if job.target_table:
                    return self._normalize_table_name(job.target_table)
                normalized = job.name.upper().replace("-", "_")
                prefix = app_config.defaults.snapshot_table_prefix
                return self._normalize_table_name(f"{prefix}{normalized}")
        raise ValueError(f"Job not found in config: {job_name}")

    def fetch_latest_rows(self, connection, table_name: str, limit: int) -> list[SnapshotRecord]:
        normalized_table = self._normalize_table_name(table_name)
        safe_limit = max(int(limit), 1)
        columns = self._fetch_table_columns(connection, normalized_table)
        canonical_columns = ["SNAPSHOT_ID", "JOB_NAME", "COLLECTED_AT_UTC", "SOURCE_SQL", "PAYLOAD_JSON"]
        cursor = connection.cursor()
        try:
            if all(column in columns for column in canonical_columns):
                cursor.execute(
                    f"""
                    SELECT SNAPSHOT_ID, JOB_NAME, COLLECTED_AT_UTC, SOURCE_SQL, PAYLOAD_JSON
                    FROM {normalized_table}
                    ORDER BY COLLECTED_AT_UTC DESC, SNAPSHOT_ID DESC
                    FETCH FIRST {safe_limit} ROWS ONLY
                    """
                )
                return [self._row_to_record(row) for row in cursor.fetchall()]

            order_column = self._pick_order_column(columns)
            select_columns = ", ".join(columns)
            order_clause = f" ORDER BY {order_column} DESC" if order_column else ""
            cursor.execute(
                f"""
                SELECT {select_columns}
                FROM {normalized_table}
                {order_clause}
                FETCH FIRST {safe_limit} ROWS ONLY
                """
            )
            return [self._generic_row_to_record(columns, row) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def fetch_latest_job_batches(
        self,
        connection,
        table_name: str,
        job_name: str,
        batch_count: int,
        limit_per_batch: int | None = None,
    ) -> list[SnapshotRecord]:
        normalized_table = self._normalize_table_name(table_name)
        safe_batch_count = max(int(batch_count), 1)
        row_limit_clause = ""
        bind_vars: dict[str, Any] = {
            "job_name": job_name,
            "batch_count": safe_batch_count,
        }
        if limit_per_batch is not None:
            safe_limit_per_batch = max(int(limit_per_batch), 1)
            row_limit_clause = "WHERE batch_row_num <= :limit_per_batch"
            bind_vars["limit_per_batch"] = safe_limit_per_batch

        cursor = connection.cursor()
        try:
            cursor.execute(
                f"""
                SELECT SNAPSHOT_ID, JOB_NAME, COLLECTED_AT_UTC, SOURCE_SQL, PAYLOAD_JSON
                FROM (
                    SELECT t.SNAPSHOT_ID,
                           t.JOB_NAME,
                           t.COLLECTED_AT_UTC,
                           t.SOURCE_SQL,
                           t.PAYLOAD_JSON,
                           ROW_NUMBER() OVER (
                               PARTITION BY t.COLLECTED_AT_UTC
                               ORDER BY t.SNAPSHOT_ID DESC
                           ) AS batch_row_num
                    FROM {normalized_table} t
                    WHERE t.JOB_NAME = :job_name
                      AND t.COLLECTED_AT_UTC IN (
                          SELECT collected_at_utc
                          FROM (
                              SELECT DISTINCT COLLECTED_AT_UTC
                              FROM {normalized_table}
                              WHERE JOB_NAME = :job_name
                              ORDER BY COLLECTED_AT_UTC DESC
                          )
                          WHERE ROWNUM <= :batch_count
                      )
                )
                {row_limit_clause}
                ORDER BY COLLECTED_AT_UTC DESC, SNAPSHOT_ID DESC
                """,
                bind_vars,
            )
            return [self._row_to_record(row) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def export_json(self, output_path: str | Path, records: list[SnapshotRecord]) -> Path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps([record.to_dict() for record in records], indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        return path

    def export_csv(self, output_path: str | Path, records: list[SnapshotRecord]) -> Path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "SNAPSHOT_ID",
                    "JOB_NAME",
                    "COLLECTED_AT_UTC",
                    "SOURCE_SQL",
                    "PAYLOAD_JSON",
                ],
            )
            writer.writeheader()
            for record in records:
                row = record.to_dict()
                row["PAYLOAD_JSON"] = self._payload_to_compact_json(row["PAYLOAD_JSON"])
                writer.writerow(row)
        return path

    @staticmethod
    def pretty_preview(records: list[SnapshotRecord]) -> list[dict[str, Any]]:
        return [record.to_dict() for record in records]

    @staticmethod
    def _row_to_record(row: tuple[Any, ...]) -> SnapshotRecord:
        snapshot_id, job_name, collected_at_utc, source_sql, payload_json = row
        source_sql_text = SnapshotReader._lob_to_text(source_sql)
        payload_text = SnapshotReader._lob_to_text(payload_json)
        return SnapshotRecord(
            snapshot_id=snapshot_id,
            job_name=job_name,
            collected_at_utc=collected_at_utc,
            source_sql=source_sql_text,
            payload_json=SnapshotReader._parse_payload(payload_text),
        )

    @staticmethod
    def _generic_row_to_record(columns: list[str], row: tuple[Any, ...]) -> SnapshotRecord:
        payload: dict[str, Any] = {}
        for column, value in zip(columns, row, strict=False):
            if isinstance(value, datetime):
                payload[column] = value.isoformat()
                continue
            text_value = SnapshotReader._lob_to_text(value)
            parsed_value = SnapshotReader._parse_payload(text_value) if isinstance(text_value, str) else value
            payload[column] = parsed_value
        return SnapshotRecord(
            snapshot_id=None,
            job_name=None,
            collected_at_utc=None,
            source_sql=None,
            payload_json=payload,
            extra_fields=payload,
        )

    @staticmethod
    def _parse_payload(payload_text: str | None) -> Any:
        if payload_text is None:
            return None
        text = payload_text.strip()
        if not text:
            return ""
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return payload_text

    @staticmethod
    def _payload_to_compact_json(payload: Any) -> str:
        if isinstance(payload, str):
            parsed = SnapshotReader._parse_payload(payload)
            if isinstance(parsed, str):
                return parsed
            payload = parsed
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)

    @staticmethod
    def _lob_to_text(value: Any) -> str | None:
        if value is None:
            return None
        if hasattr(value, "read"):
            return value.read()
        return str(value)

    @staticmethod
    def _normalize_table_name(table_name: str) -> str:
        normalized = table_name.strip().upper()
        if not _IDENTIFIER_RE.match(normalized):
            raise ValueError(f"Unsafe or unsupported table name: {table_name}")
        return normalized

    @staticmethod
    def _fetch_table_columns(connection, table_name: str) -> list[str]:
        cursor = connection.cursor()
        try:
            cursor.execute(
                """
                SELECT column_name
                FROM user_tab_columns
                WHERE table_name = :table_name
                ORDER BY column_id
                """,
                table_name=table_name,
            )
            return [row[0] for row in cursor.fetchall()]
        finally:
            cursor.close()

    @staticmethod
    def _pick_order_column(columns: list[str]) -> str | None:
        for candidate in (
            "COLLECTED_AT_UTC",
            "CREATED_AT_UTC",
            "CREATED_AT",
            "STARTED_AT_UTC",
            "PLANNED_AT_UTC",
            "LOG_ID",
            "SCHEDULE_RUN_ID",
            "SNAPSHOT_ID",
        ):
            if candidate in columns:
                return candidate
        return None


def records_to_json_text(records: list[SnapshotRecord]) -> str:
    return json.dumps(SnapshotReader.pretty_preview(records), indent=2, ensure_ascii=False)


def records_to_csv_text(records: list[SnapshotRecord]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=["SNAPSHOT_ID", "JOB_NAME", "COLLECTED_AT_UTC", "SOURCE_SQL", "PAYLOAD_JSON"],
    )
    writer.writeheader()
    reader = SnapshotReader()
    for record in records:
        row = record.to_dict()
        row["PAYLOAD_JSON"] = reader._payload_to_compact_json(row["PAYLOAD_JSON"])
        writer.writerow(row)
    return buffer.getvalue()
