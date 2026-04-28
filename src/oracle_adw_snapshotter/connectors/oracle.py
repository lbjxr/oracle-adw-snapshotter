from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import socket

try:
    import oracledb
except ModuleNotFoundError:  # pragma: no cover - allows import smoke tests without dependency
    oracledb = None  # type: ignore[assignment]

from oracle_adw_snapshotter.models.types import DatabaseConfig


class OracleConnectionConfigError(ValueError):
    """Raised when Oracle connection settings are incomplete or inconsistent."""


class OracleConnector:
    _DISCONNECT_ERROR_MARKERS = (
        "DPY-4011",
        "DPY-1001",
        "DPI-1080",
        "DPI-1010",
        "ORA-03113",
        "ORA-03114",
        "ORA-03135",
        "ORA-12170",
        "ORA-12537",
        "ORA-12547",
        "ORA-12570",
        "ORA-12571",
        "ORA-12637",
        "connection reset",
        "connection closed",
        "network closed the connection",
        "not connected",
        "socket is closed",
        "broken pipe",
    )

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._client_initialized = False

    @property
    def connection_mode(self) -> str:
        return (self.config.connection_mode or "thin").strip().lower()

    @property
    def effective_config_dir(self) -> str | None:
        return self.config.config_dir or self.config.wallet_dir

    def validate_configuration(self) -> dict[str, Any]:
        mode = self.connection_mode
        if mode not in {"thin", "thick"}:
            raise OracleConnectionConfigError(
                "ORACLE_CONNECTION_MODE must be either 'thin' or 'thick'"
            )

        wallet_dir = Path(self.config.wallet_dir).expanduser() if self.config.wallet_dir else None
        config_dir = Path(self.effective_config_dir).expanduser() if self.effective_config_dir else None
        lib_dir = Path(self.config.lib_dir).expanduser() if self.config.lib_dir else None

        checks: list[dict[str, Any]] = []
        warnings: list[str] = []

        def add_check(name: str, ok: bool, detail: str) -> None:
            checks.append({"name": name, "ok": ok, "detail": detail})

        if self.config.wallet_password and not wallet_dir:
            raise OracleConnectionConfigError(
                "ORACLE_WALLET_PASSWORD is set but ORACLE_WALLET_DIR is empty"
            )

        if wallet_dir:
            add_check("wallet_dir", wallet_dir.exists(), f"wallet_dir={wallet_dir}")
            if not wallet_dir.exists():
                raise OracleConnectionConfigError(f"Wallet directory does not exist: {wallet_dir}")
            if not wallet_dir.is_dir():
                raise OracleConnectionConfigError(f"Wallet path is not a directory: {wallet_dir}")

        if config_dir:
            add_check("config_dir", config_dir.exists(), f"config_dir={config_dir}")
            if not config_dir.exists():
                raise OracleConnectionConfigError(f"Config directory does not exist: {config_dir}")
            if not config_dir.is_dir():
                raise OracleConnectionConfigError(f"Config path is not a directory: {config_dir}")

            tnsnames = config_dir / "tnsnames.ora"
            sqlnet = config_dir / "sqlnet.ora"
            add_check("tnsnames", tnsnames.exists(), f"expected={tnsnames}")
            add_check("sqlnet", sqlnet.exists(), f"expected={sqlnet}")
            if not tnsnames.exists():
                raise OracleConnectionConfigError(
                    f"tnsnames.ora not found under config_dir: {config_dir}"
                )
            if wallet_dir and not sqlnet.exists():
                warnings.append(
                    "sqlnet.ora was not found. Some wallet-based ADW setups need it for TCPS/mTLS."
                )

        if mode == "thick":
            if lib_dir:
                add_check("lib_dir", lib_dir.exists(), f"lib_dir={lib_dir}")
                if not lib_dir.exists():
                    raise OracleConnectionConfigError(
                        f"Oracle Client lib_dir does not exist: {lib_dir}"
                    )
                if not lib_dir.is_dir():
                    raise OracleConnectionConfigError(
                        f"Oracle Client lib_dir is not a directory: {lib_dir}"
                    )
            else:
                warnings.append(
                    "Thick mode selected without ORACLE_LIB_DIR. This is fine only if Oracle Instant Client is already discoverable via the system library path."
                )

        if self.connection_mode == "thin" and self.config.lib_dir:
            warnings.append(
                "ORACLE_LIB_DIR is set but ORACLE_CONNECTION_MODE=thin. It will be ignored unless you switch to thick mode."
            )

        dsn_looks_like_alias = "/" not in self.config.dsn and ":" not in self.config.dsn and "(" not in self.config.dsn
        if dsn_looks_like_alias and not config_dir:
            warnings.append(
                "DSN looks like a TNS alias, but neither ORACLE_CONFIG_DIR nor ORACLE_WALLET_DIR is set. Alias resolution may fail."
            )

        return {
            "connection_mode": mode,
            "wallet_dir": str(wallet_dir) if wallet_dir else None,
            "config_dir": str(config_dir) if config_dir else None,
            "lib_dir": str(lib_dir) if lib_dir else None,
            "checks": checks,
            "warnings": warnings,
        }

    def _ensure_client(self) -> None:
        if self._client_initialized or self.connection_mode != "thick":
            return
        if oracledb is None:
            raise RuntimeError("python-oracledb is not installed")
        if self.config.lib_dir:
            oracledb.init_oracle_client(lib_dir=self.config.lib_dir)
        else:
            oracledb.init_oracle_client()
        self._client_initialized = True

    def connect(self):
        if oracledb is None:
            raise RuntimeError("python-oracledb is not installed")

        self.validate_configuration()
        self._ensure_client()
        connect_kwargs: dict[str, Any] = {
            "user": self.config.user,
            "password": self.config.password,
            "dsn": self.config.dsn,
            "expire_time": self.config.expire_time_minutes,
            "retry_count": self.config.retry_count,
            "retry_delay": self.config.retry_delay_seconds,
            "tcp_connect_timeout": self.config.tcp_connect_timeout_seconds,
        }
        if self.config.wallet_dir:
            connect_kwargs["wallet_location"] = self.config.wallet_dir
        if self.config.wallet_password:
            connect_kwargs["wallet_password"] = self.config.wallet_password
        if self.effective_config_dir:
            connect_kwargs["config_dir"] = self.effective_config_dir

        return oracledb.connect(**connect_kwargs)

    def test_connection(self) -> dict[str, Any]:
        report = self.validate_configuration()
        report["driver_available"] = oracledb is not None
        report["connection_ok"] = False

        if oracledb is None:
            raise RuntimeError("python-oracledb is not installed")

        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        SYS_CONTEXT('USERENV', 'DB_NAME') AS db_name,
                        SYS_CONTEXT('USERENV', 'SERVICE_NAME') AS service_name,
                        SYS_CONTEXT('USERENV', 'SESSION_USER') AS session_user
                    FROM dual
                    """
                )
                row = cursor.fetchone()

        report["connection_ok"] = True
        report["database"] = {
            "db_name": row[0] if row else None,
            "service_name": row[1] if row else None,
            "session_user": row[2] if row else None,
        }
        return report

    @staticmethod
    def is_disconnect_error(exc: BaseException) -> bool:
        current: BaseException | None = exc
        while current is not None:
            if isinstance(current, (ConnectionError, BrokenPipeError, TimeoutError, socket.timeout)):
                return True
            message = str(current).upper()
            if any(marker.upper() in message for marker in OracleConnector._DISCONNECT_ERROR_MARKERS):
                return True
            current = current.__cause__ or current.__context__
        return False

    @contextmanager
    def session(self) -> Iterator[Any]:
        connection = self.connect()
        try:
            yield connection
        finally:
            connection.close()
