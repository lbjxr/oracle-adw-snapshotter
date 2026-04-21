from __future__ import annotations

from pathlib import Path

from oracle_adw_snapshotter.cli import build_parser
from oracle_adw_snapshotter.config.settings import load_app_config
from oracle_adw_snapshotter.connectors.oracle import OracleConnectionConfigError, OracleConnector


def test_load_app_config_smoke(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "ORACLE_USER=test_user",
                "ORACLE_PASSWORD=test_password",
                "ORACLE_DSN=example_high",
                "ORACLE_CONNECTION_MODE=thin",
                "SNAPSHOT_APP_NAME=test-snapshotter",
                "SNAPSHOT_FETCH_SIZE=250",
            ]
        ),
        encoding="utf-8",
    )

    config_file = tmp_path / "tasks.yaml"
    config_file.write_text(
        """
defaults:
  target_schema: SNAPSHOT_ARCHIVE
jobs:
  - name: demo_job
    enabled: true
    mode: query
    source_sql: |
      SELECT 1 AS sample_col FROM dual
    target_table: SNAP_DEMO_JOB
""".strip(),
        encoding="utf-8",
    )

    monkeypatch.delenv("ORACLE_USER", raising=False)
    monkeypatch.delenv("ORACLE_PASSWORD", raising=False)
    monkeypatch.delenv("ORACLE_DSN", raising=False)

    app_config = load_app_config(config_path=str(config_file), env_file=str(env_file))

    assert app_config.app_name == "test-snapshotter"
    assert app_config.db.fetch_size == 250
    assert app_config.db.connection_mode == "thin"
    assert len(app_config.jobs) == 1
    assert app_config.jobs[0].resolved_query().startswith("SELECT 1")
    parser = build_parser()
    parsed = parser.parse_args(["print-config", "--config", str(config_file), "--env-file", str(env_file)])
    assert parsed.command == "print-config"

    test_conn = parser.parse_args(["test-connection", "--config", str(config_file), "--env-file", str(env_file)])
    assert test_conn.command == "test-connection"


def test_connector_validation_uses_wallet_dir_as_default_config_dir(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("ORACLE_USER", raising=False)
    monkeypatch.delenv("ORACLE_PASSWORD", raising=False)
    monkeypatch.delenv("ORACLE_DSN", raising=False)
    monkeypatch.delenv("ORACLE_CONNECTION_MODE", raising=False)
    monkeypatch.delenv("ORACLE_WALLET_DIR", raising=False)
    monkeypatch.delenv("ORACLE_CONFIG_DIR", raising=False)
    monkeypatch.delenv("ORACLE_WALLET_PASSWORD", raising=False)
    monkeypatch.delenv("ORACLE_LIB_DIR", raising=False)

    wallet_dir = tmp_path / "wallet"
    wallet_dir.mkdir()
    (wallet_dir / "tnsnames.ora").write_text("demo_high=(DESCRIPTION=...)", encoding="utf-8")
    (wallet_dir / "sqlnet.ora").write_text("WALLET_LOCATION=(SOURCE=(METHOD=file)))", encoding="utf-8")

    app_config = load_app_config(
        config_path=str(_write_config(tmp_path)),
        env_file=str(
            _write_env(
                tmp_path,
                [
                    "ORACLE_USER=test_user",
                    "ORACLE_PASSWORD=test_password",
                    "ORACLE_DSN=demo_high",
                    "ORACLE_CONNECTION_MODE=thin",
                    f"ORACLE_WALLET_DIR={wallet_dir}",
                ],
            )
        ),
    )

    report = OracleConnector(app_config.db).validate_configuration()
    assert report["config_dir"] == str(wallet_dir)
    assert any(check["name"] == "tnsnames" and check["ok"] for check in report["checks"])


def test_connector_validation_rejects_missing_wallet_dir(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("ORACLE_USER", raising=False)
    monkeypatch.delenv("ORACLE_PASSWORD", raising=False)
    monkeypatch.delenv("ORACLE_DSN", raising=False)
    monkeypatch.delenv("ORACLE_CONNECTION_MODE", raising=False)
    monkeypatch.delenv("ORACLE_WALLET_DIR", raising=False)
    monkeypatch.delenv("ORACLE_CONFIG_DIR", raising=False)
    monkeypatch.delenv("ORACLE_WALLET_PASSWORD", raising=False)
    monkeypatch.delenv("ORACLE_LIB_DIR", raising=False)

    missing_wallet = tmp_path / "missing-wallet"
    app_config = load_app_config(
        config_path=str(_write_config(tmp_path)),
        env_file=str(
            _write_env(
                tmp_path,
                [
                    "ORACLE_USER=test_user",
                    "ORACLE_PASSWORD=test_password",
                    "ORACLE_DSN=demo_high",
                    f"ORACLE_WALLET_DIR={missing_wallet}",
                ],
            )
        ),
    )

    connector = OracleConnector(app_config.db)
    try:
        connector.validate_configuration()
    except OracleConnectionConfigError as exc:
        assert "Wallet directory does not exist" in str(exc)
    else:
        raise AssertionError("Expected wallet validation to fail")


def _write_config(tmp_path: Path) -> Path:
    config_file = tmp_path / "tasks.yaml"
    config_file.write_text(
        """
defaults:
  target_schema: SNAPSHOT_ARCHIVE
jobs:
  - name: demo_job
    enabled: true
    mode: query
    source_sql: |
      SELECT 1 AS sample_col FROM dual
    target_table: SNAP_DEMO_JOB
""".strip(),
        encoding="utf-8",
    )
    return config_file


def _write_env(tmp_path: Path, lines: list[str]) -> Path:
    env_file = tmp_path / ".env"
    env_file.write_text("\n".join(lines), encoding="utf-8")
    return env_file
