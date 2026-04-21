from __future__ import annotations

import csv
import io
import json
from datetime import datetime, timezone
from pathlib import Path

from oracle_adw_snapshotter.cli import build_parser
from oracle_adw_snapshotter.config.settings import load_app_config
from oracle_adw_snapshotter.storage.snapshot_reader import (
    SnapshotReader,
    SnapshotRecord,
    records_to_csv_text,
    records_to_json_text,
)


def test_cli_parser_accepts_view_and_export(tmp_path: Path, monkeypatch) -> None:
    config_file, env_file = _write_fixture_files(tmp_path)

    monkeypatch.delenv("ORACLE_USER", raising=False)
    monkeypatch.delenv("ORACLE_PASSWORD", raising=False)
    monkeypatch.delenv("ORACLE_DSN", raising=False)

    app_config = load_app_config(config_path=str(config_file), env_file=str(env_file))
    reader = SnapshotReader()

    parser = build_parser()
    view_args = parser.parse_args(
        [
            "view",
            "--config",
            str(config_file),
            "--env-file",
            str(env_file),
            "--job",
            "demo_job",
            "--latest-runs",
            "2",
            "--rows-per-run",
            "5",
        ]
    )
    assert view_args.command == "view"
    assert view_args.latest_runs == 2
    assert view_args.rows_per_run == 5
    assert reader.resolve_table_for_job(app_config, "demo_job") == "SNAP_DEMO_JOB"

    export_args = parser.parse_args(
        [
            "export",
            "--config",
            str(config_file),
            "--env-file",
            str(env_file),
            "--table",
            "SNAP_DEMO_JOB",
            "--format",
            "csv",
            "--output",
            str(tmp_path / "out.csv"),
        ]
    )
    assert export_args.command == "export"
    assert export_args.output_format == "csv"


def test_snapshot_reader_pretty_payload_and_export(tmp_path: Path) -> None:
    collected_at = datetime(2026, 4, 21, 7, 0, tzinfo=timezone.utc)
    records = [
        SnapshotRecord(
            snapshot_id=101,
            job_name="demo_job",
            collected_at_utc=collected_at,
            source_sql="SELECT 1 FROM dual",
            payload_json={"sample": 1, "nested": {"ok": True}},
        ),
        SnapshotRecord(
            snapshot_id=102,
            job_name="demo_job",
            collected_at_utc=collected_at,
            source_sql="SELECT 1 FROM dual",
            payload_json=json.loads('{"sample":2,"text":"hello"}'),
        ),
    ]

    json_text = records_to_json_text(records)
    assert '"nested"' in json_text
    assert '"text": "hello"' in json_text

    csv_text = records_to_csv_text(records)
    assert "SNAPSHOT_ID,JOB_NAME,COLLECTED_AT_UTC,SOURCE_SQL,PAYLOAD_JSON" in csv_text
    csv_rows = list(csv.DictReader(io.StringIO(csv_text)))
    assert len(csv_rows) == 2
    assert json.loads(csv_rows[0]["PAYLOAD_JSON"]) == {"sample": 1, "nested": {"ok": True}}

    reader = SnapshotReader()
    json_path = reader.export_json(tmp_path / "exports" / "snap.json", records)
    csv_path = reader.export_csv(tmp_path / "exports" / "snap.csv", records)

    assert json_path.exists()
    assert csv_path.exists()
    assert '"SNAPSHOT_ID": 101' in json_path.read_text(encoding="utf-8")
    exported_rows = list(csv.DictReader(io.StringIO(csv_path.read_text(encoding="utf-8"))))
    assert json.loads(exported_rows[1]["PAYLOAD_JSON"]) == {"sample": 2, "text": "hello"}


def _write_fixture_files(tmp_path: Path) -> tuple[Path, Path]:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "ORACLE_USER=test_user",
                "ORACLE_PASSWORD=test_password",
                "ORACLE_DSN=example_high",
                "ORACLE_CONNECTION_MODE=thin",
            ]
        ),
        encoding="utf-8",
    )

    config_file = tmp_path / "tasks.yaml"
    config_file.write_text(
        """
defaults:
  snapshot_table_prefix: SNAP_
jobs:
  - name: demo_job
    enabled: true
    mode: query
    source_sql: |
      SELECT 1 AS sample_col FROM dual
""".strip(),
        encoding="utf-8",
    )
    return config_file, env_file
