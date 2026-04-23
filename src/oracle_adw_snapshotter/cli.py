from __future__ import annotations

import argparse
import json
from dataclasses import asdict

from oracle_adw_snapshotter.config.settings import load_app_config
from oracle_adw_snapshotter.connectors.oracle import OracleConnectionConfigError, OracleConnector
from oracle_adw_snapshotter.jobs.random_runner import RandomizedExecutionRunner
from oracle_adw_snapshotter.jobs.runner import SnapshotJobRunner
from oracle_adw_snapshotter.services.query_service import QueryService
from oracle_adw_snapshotter.services.scheduler_loop import RandomSchedulerLoop
from oracle_adw_snapshotter.storage.snapshot_reader import (
    SnapshotReader,
    records_to_csv_text,
    records_to_json_text,
)
from oracle_adw_snapshotter.storage.snapshot_writer import SnapshotWriter


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Oracle ADW snapshotter")
    parser.add_argument(
        "command",
        choices=["run", "print-config", "test-connection", "view", "export", "scheduler"],
        help="Command to execute",
    )
    parser.add_argument("--config", default=None, help="Path to tasks YAML")
    parser.add_argument("--env-file", default=None, help="Path to .env file")
    parser.add_argument("--job", action="append", dest="jobs", default=None, help="Only run named job(s)")
    parser.add_argument("--table", default=None, help="Snapshot table name to inspect or export")
    parser.add_argument("--limit", type=int, default=10, help="Maximum records to view or export")
    parser.add_argument(
        "--latest-runs",
        type=int,
        default=1,
        help="How many recent collection batches to include when used with --job",
    )
    parser.add_argument(
        "--rows-per-run",
        type=int,
        default=None,
        help="Optional per-batch row cap when used with --job",
    )
    parser.add_argument(
        "--format",
        dest="output_format",
        choices=["json", "csv"],
        default="json",
        help="Output format for export, or for view when you want CSV-style preview",
    )
    parser.add_argument("--output", default=None, help="File path for export output")
    parser.add_argument("--once", action="store_true", help="For scheduler: run only due items and exit")
    return parser


def _print_json(payload: object) -> None:
    print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))


def _load_or_print_config(args) -> tuple[object | None, int | None]:
    try:
        app_config = load_app_config(config_path=args.config, env_file=args.env_file)
        return app_config, None
    except Exception as exc:
        _print_json(
            {
                "ok": False,
                "stage": "load-config",
                "error_type": exc.__class__.__name__,
                "error": str(exc),
            }
        )
        return None, 2


def _resolve_view_records(connection, app_config, args, reader: SnapshotReader):
    if args.jobs:
        if len(args.jobs) != 1:
            raise ValueError("view/export currently expects exactly one --job when querying by job")
        job_name = args.jobs[0]
        table_name = args.table or reader.resolve_table_for_job(app_config, job_name)
        return reader.fetch_latest_job_batches(
            connection=connection,
            table_name=table_name,
            job_name=job_name,
            batch_count=args.latest_runs,
            limit_per_batch=args.rows_per_run,
        )

    if not args.table:
        raise ValueError("Either --table or --job must be provided for view/export")
    return reader.fetch_latest_rows(connection=connection, table_name=args.table, limit=args.limit)


def _handle_view(connection, app_config, args, reader: SnapshotReader) -> None:
    records = _resolve_view_records(connection, app_config, args, reader)
    if args.output_format == "csv":
        print(records_to_csv_text(records), end="")
        return
    print(records_to_json_text(records))


def _handle_export(connection, app_config, args, reader: SnapshotReader) -> None:
    if not args.output:
        raise ValueError("--output is required for export")
    records = _resolve_view_records(connection, app_config, args, reader)
    if not args.jobs and args.limit is not None:
        records = records[: max(int(args.limit), 1)]
    if args.output_format == "csv":
        output_path = reader.export_csv(args.output, records)
    else:
        output_path = reader.export_json(args.output, records)
    _print_json(
        {
            "ok": True,
            "command": "export",
            "format": args.output_format,
            "output": str(output_path),
            "rows": len(records),
        }
    )


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    app_config, error_code = _load_or_print_config(args)
    if error_code is not None:
        return error_code
    assert app_config is not None

    if args.command == "print-config":
        _print_json(asdict(app_config))
        return 0

    connector = OracleConnector(app_config.db)

    if args.command == "test-connection":
        try:
            report = connector.test_connection()
        except OracleConnectionConfigError as exc:
            _print_json(
                {
                    "ok": False,
                    "stage": "validate-config",
                    "error_type": exc.__class__.__name__,
                    "error": str(exc),
                    "connection_mode": connector.connection_mode,
                    "dsn": app_config.db.dsn,
                }
            )
            return 2
        except Exception as exc:
            _print_json(
                {
                    "ok": False,
                    "stage": "connect",
                    "error_type": exc.__class__.__name__,
                    "error": str(exc),
                    "connection_mode": connector.connection_mode,
                    "dsn": app_config.db.dsn,
                }
            )
            return 1

        _print_json(
            {
                "ok": True,
                "stage": "connect",
                "app_name": app_config.app_name,
                "dsn": app_config.db.dsn,
                **report,
            }
        )
        return 0

    if args.command in {"view", "export"}:
        reader = SnapshotReader()
        try:
            with connector.session() as connection:
                if args.command == "view":
                    _handle_view(connection, app_config, args, reader)
                else:
                    _handle_export(connection, app_config, args, reader)
        except OracleConnectionConfigError as exc:
            _print_json(
                {
                    "ok": False,
                    "stage": "validate-config",
                    "error_type": exc.__class__.__name__,
                    "error": str(exc),
                }
            )
            return 2
        except Exception as exc:
            _print_json(
                {
                    "ok": False,
                    "stage": args.command,
                    "error_type": exc.__class__.__name__,
                    "error": str(exc),
                }
            )
            return 1
        return 0

    if args.command == "scheduler":
        scheduler_loop = RandomSchedulerLoop(execution_runner=RandomizedExecutionRunner())
        try:
            results = scheduler_loop.run_forever(connect=connector.session, app_config=app_config, once=args.once)
        except OracleConnectionConfigError as exc:
            _print_json(
                {
                    "ok": False,
                    "stage": "validate-config",
                    "error_type": exc.__class__.__name__,
                    "error": str(exc),
                }
            )
            return 2
        except Exception as exc:
            _print_json(
                {
                    "ok": False,
                    "stage": "scheduler",
                    "error_type": exc.__class__.__name__,
                    "error": str(exc),
                }
            )
            return 1
        _print_json({"ok": True, "command": "scheduler", "results": results})
        return 0

    runner = SnapshotJobRunner(
        query_service=QueryService(fetch_size=app_config.db.fetch_size),
        snapshot_writer=SnapshotWriter(),
    )

    try:
        with connector.session() as connection:
            results = runner.run_jobs(connection=connection, app_config=app_config, job_names=args.jobs)
            connection.commit()
    except OracleConnectionConfigError as exc:
        _print_json(
            {
                "ok": False,
                "stage": "validate-config",
                "error_type": exc.__class__.__name__,
                "error": str(exc),
            }
        )
        return 2
    except Exception as exc:
        _print_json(
            {
                "ok": False,
                "stage": "run",
                "error_type": exc.__class__.__name__,
                "error": str(exc),
            }
        )
        return 1

    _print_json([asdict(result) for result in results])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
