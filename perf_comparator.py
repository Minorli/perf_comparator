#!/usr/bin/env python3
"""Single-file runtime foundation for perf_comparator."""

from __future__ import print_function

import argparse
import atexit
import base64
import configparser
import hashlib
import html
import json
import logging
import math
import os
import re
import shlex
import ssl
import stat
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

try:
    import oracledb  # type: ignore
except Exception:  # pragma: no cover - optional at runtime in tests
    oracledb = None


MODE_BATCH = "batch"
MODE_STREAM = "stream"
MODE_REPLAY_ONLY = "replay-only"
MODE_REPORT_ONLY = "report-only"
MODE_CHECK_CONFIG = "check-config"
MODE_SOURCE_REPORT = "source-report"
MODE_VERIFY_REALDB = "verify-realdb"

DEFAULT_WORKLOADS_DIR = "workloads"
DEFAULT_REPORT_DIR = "reports"
DEFAULT_TOP_N = 50
DEFAULT_MIN_EXEC = 5
DEFAULT_HOURS = 24
DEFAULT_TIMEOUT_FACTOR = 3.0
DEFAULT_SLOWDOWN_THRESHOLD = 0.8
DEFAULT_INTERVAL = 60
DEFAULT_ROLLING_REPORT_INTERVAL = 300
DEFAULT_CAPTURE_TOP_N = 1000
DEFAULT_AUDIT_POLL_MS = 300
DEFAULT_OBCLIENT_TIMEOUT = 120
DEFAULT_OB_SESSION_QUERY_TIMEOUT_US = 3600000000
DEFAULT_PLSQL_PROFILE_TOP_N = 10
DEFAULT_PLSQL_PROFILE_SOURCE_CONTEXT = 1
DEFAULT_OCP_TIMEOUT = 15
DEFAULT_OBDIAG_TIMEOUT = 120
SOURCE_TEXT_CR_SENTINEL = "\x1e"
SOURCE_TEXT_LF_SENTINEL = "\x1f"
DEFAULT_SOURCE_ACTOR_FIELDS = ["tenant_name", "db_name", "user_name", "user_client_ip"]

SECTION_ORACLE_SOURCE = "ORACLE_SOURCE"
SECTION_OCEANBASE_SOURCE = "OCEANBASE_SOURCE"
SECTION_OCEANBASE_SOURCE_SYS = "OCEANBASE_SOURCE_SYS"
SECTION_OCEANBASE_TARGET = "OCEANBASE_TARGET"
SECTION_SETTINGS = "SETTINGS"

SOURCE_DB_MODE_ORACLE = "oracle"
SOURCE_DB_MODE_OCEANBASE = "oceanbase"

ARTIFACT_SPECS = {
    "workload": ("workload", ".jsonl"),
    "replay": ("replay", ".jsonl"),
    "audit_dump": ("audit_dump", ".jsonl"),
    "capture_capability": ("capture_capability", ".json"),
    "replay_capability": ("replay_capability", ".json"),
    "plsql_profile": ("plsql_profile", ".jsonl"),
    "mismatch": ("mismatch", ".jsonl"),
    "realdb_verify": ("realdb_verify", ".json"),
    "report_html": ("perf_report", ".html"),
    "report_summary": ("perf_report", "_summary.txt"),
    "report_hints": ("perf_hints", ".sql"),
}

PLAN_TYPE_NAMES = {
    "1": "LOCAL",
    "2": "REMOTE",
    "3": "DISTRIBUTED",
    "4": "UNCERTAIN",
}

LOG = logging.getLogger("perf_comparator")
_SECURE_FILES = set()  # type: ignore[var-annotated]
_SECURE_FILES_LOCK = threading.Lock()

PROFILER_TEST_PACKAGE_NAME = "TEST_PROFILER_PKG"
DEFAULT_REALDB_ORACLE_CONFIG = "/home/minorli/comparator/config.ini"
DEFAULT_REALDB_OB_SOURCE_CONFIG = "/home/minorli/comparator/config.ini.ob"
DEFAULT_REALDB_PROFILER_CALL = "CALL TEST_PROFILER_PKG.run_workload()"

PROFILER_TEST_PACKAGE_SQL = """
CREATE OR REPLACE PACKAGE TEST_PROFILER_PKG AS
    PROCEDURE run_workload;
END;
/
CREATE OR REPLACE PACKAGE BODY TEST_PROFILER_PKG AS
    PROCEDURE burn_cpu IS
        v_temp NUMBER := 0;
    BEGIN
        FOR i IN 1..50000 LOOP
            v_temp := SQRT(i);
        END LOOP;
    END;

    PROCEDURE run_workload IS
    BEGIN
        burn_cpu;
        burn_cpu;
    END;
END;
/
"""


class ConfigError(Exception):
    """Raised when config.ini is invalid."""


@dataclass
class AppConfig:
    oracle_source: Dict[str, str]
    oceanbase_source: Dict[str, str]
    oceanbase_target: Dict[str, str]
    settings: Dict[str, Any]
    config_path: str
    oceanbase_source_sys: Dict[str, str] = field(default_factory=dict)


@dataclass
class PreflightResult:
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    @property
    def ok(self):
        # type: () -> bool
        return not self.errors


class SQLAuditCollector(object):
    """Collects GV$OB_SQL_AUDIT rows in the background."""

    def __init__(self, config, audit_dump_path):
        # type: (AppConfig, Union[str, Path]) -> None
        self.config = config
        self.audit_dump_path = Path(audit_dump_path)
        self.poll_interval = max(
            0.1, float(config.settings.get("audit_poll_ms", DEFAULT_AUDIT_POLL_MS)) / 1000.0
        )
        self.last_request_id = 0
        self._rows = []  # type: List[Dict[str, Any]]
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread = None  # type: Optional[threading.Thread]

    def _build_query(self):
        # type: () -> str
        return """
            SELECT
              REQUEST_ID,
              TRACE_ID,
              SQL_ID,
              ELAPSED_TIME,
              QUEUE_TIME,
              GET_PLAN_TIME,
              EXECUTE_TIME,
              NET_TIME,
              NET_WAIT_TIME,
              PLAN_TYPE,
              IS_HIT_PLAN,
              IS_EXECUTOR_RPC,
              LOGICAL_READ_COUNT,
              PHYSICAL_READ_COUNT,
              RETRY_CNT,
              MEMSTORE_READ_ROW_COUNT,
              SSSTORE_READ_ROW_COUNT,
              BLOOM_FILTER_FILTERED_COUNT,
              SQL_TEXT
            FROM GV$OB_SQL_AUDIT
            WHERE REQUEST_ID > {last_request_id}
            ORDER BY REQUEST_ID
        """.format(last_request_id=int(self.last_request_id))

    def collect_once(self):
        # type: () -> List[Dict[str, Any]]
        ok, stdout, _ = obclient_run_sql(
            self.config.oceanbase_target,
            self._build_query(),
            timeout=self.config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT),
            session_query_timeout_us=self.config.settings.get(
                "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
            ),
        )
        if not ok or not stdout.strip():
            return []
        rows = []
        for line in stdout.splitlines():
            fields = line.split("\t")
            if len(fields) < 15:
                continue
            row = {
                "request_id": int(fields[0]),
                "trace_id": fields[1],
                "sql_id": fields[2],
                "ob_elapsed_us": _safe_float(fields[3]),
                "ob_queue_time_us": _safe_float(fields[4]),
                "ob_get_plan_time_us": _safe_float(fields[5]),
                "ob_execute_time_us": _safe_float(fields[6]),
                "ob_net_time_us": _safe_float(fields[7]),
                "ob_net_wait_time_us": _safe_float(fields[8]),
                "ob_plan_type_raw": fields[9],
                "ob_is_hit_plan": fields[10],
                "ob_is_executor_rpc": fields[11],
                "ob_logical_reads": _safe_float(fields[12]),
                "ob_physical_reads": _safe_float(fields[13]),
                "sql_text": fields[14],
                "ob_retry_cnt": _safe_float(fields[15]) if len(fields) > 15 else None,
                "ob_memstore_read_rows": _safe_float(fields[16]) if len(fields) > 16 else None,
                "ob_ssstore_read_rows": _safe_float(fields[17]) if len(fields) > 17 else None,
                "ob_bloom_filter_filtered": _safe_float(fields[18]) if len(fields) > 18 else None,
                "captured_at": utc_now_iso(),
            }
            rows.append(row)
        if not rows:
            return []
        self.last_request_id = max(row["request_id"] for row in rows)
        append_jsonl(self.audit_dump_path, rows)
        with self._lock:
            self._rows.extend(rows)
        return rows

    def _run(self):
        # type: () -> None
        while not self._stop_event.is_set():
            try:
                self.collect_once()
            except Exception:
                LOG.exception("SQL Audit collector poll failed")
            self._stop_event.wait(self.poll_interval)

    def start(self):
        # type: () -> None
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="sql-audit-collector", daemon=True)
        self._thread.start()

    def stop(self):
        # type: () -> None
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self.poll_interval * 2.0))
        try:
            self.collect_once()
        except Exception:
            LOG.exception("SQL Audit collector final flush failed")

    def match_for_sql(self, rendered_sql):
        # type: (str) -> Dict[str, Any]
        normalized = normalize_sql_text(rendered_sql)
        with self._lock:
            candidates = [
                row
                for row in self._rows
                if normalized and normalized in normalize_sql_text(row.get("sql_text", ""))
            ]
        if not candidates:
            return {}
        return sorted(candidates, key=lambda item: item.get("request_id") or 0)[-1]

    def match_for_workload(self, workload_row, rendered_sql):
        # type: (Dict[str, Any], str) -> Dict[str, Any]
        sql_id = str(workload_row.get("sql_id") or "")
        normalized = normalize_sql_text(rendered_sql)
        with self._lock:
            if sql_id:
                exact = [row for row in self._rows if str(row.get("sql_id") or "") == sql_id]
                if exact:
                    return sorted(exact, key=lambda item: item.get("request_id") or 0)[-1]
            candidates = [
                row
                for row in self._rows
                if normalized and normalized in normalize_sql_text(row.get("sql_text", ""))
            ]
        if not candidates:
            return {}
        return sorted(candidates, key=lambda item: item.get("request_id") or 0)[-1]


class ReplayBackend(object):
    """Backend abstraction for replay execution."""

    name = "base"

    def execute(self, config, rendered_sql, timeout_seconds):
        # type: (AppConfig, str, int) -> Tuple[bool, str, str]
        raise NotImplementedError

    def explain(self, config, rendered_sql):
        # type: (AppConfig, str) -> Tuple[bool, str, str]
        raise NotImplementedError


class ObclientReplayBackend(ReplayBackend):
    name = "obclient"

    def execute(self, config, rendered_sql, timeout_seconds):
        # type: (AppConfig, str, int) -> Tuple[bool, str, str]
        return obclient_run_sql(
            config.oceanbase_target,
            rendered_sql,
            timeout=timeout_seconds,
            session_query_timeout_us=config.settings.get(
                "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
            ),
        )

    def explain(self, config, rendered_sql):
        # type: (AppConfig, str) -> Tuple[bool, str, str]
        return obclient_run_sql(
            config.oceanbase_target,
            "EXPLAIN EXTENDED " + rendered_sql,
            timeout=config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT),
            session_query_timeout_us=config.settings.get(
                "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
            ),
        )


def configure_logging(level_name):
    # type: (str) -> None
    level = getattr(logging, str(level_name or "INFO").strip().upper(), logging.INFO)
    root = logging.getLogger()
    if not root.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        root.addHandler(handler)
    root.setLevel(level)


def normalize_schema_list(value):
    # type: (str) -> List[str]
    if not value:
        return []
    return [item.strip().upper() for item in str(value).split(",") if item.strip()]


def normalize_csv_list(value):
    # type: (Any) -> List[str]
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    if not value:
        return []
    return [item.strip() for item in str(value).split(",") if item.strip()]


def is_sys_ob_user_string(user_string):
    # type: (Any) -> bool
    normalized = str(user_string or "").strip().upper()
    return normalized.startswith("SYS@")


def get_capture_top_n(config):
    # type: (AppConfig) -> int
    configured = int(config.settings.get("capture_top_n", 0) or 0)
    if configured > 0:
        return configured
    return max(int(config.settings.get("top_n", DEFAULT_TOP_N) or DEFAULT_TOP_N), DEFAULT_CAPTURE_TOP_N)


def build_query_sql_visibility_warning(config):
    # type: (AppConfig) -> str
    if config.settings.get("source_db_mode") != SOURCE_DB_MODE_OCEANBASE:
        return ""
    source_user = str(config.oceanbase_source.get("user_string") or "").strip()
    if not source_user or is_sys_ob_user_string(source_user):
        return ""
    if config.oceanbase_source_sys:
        return (
            "QUERY_SQL visibility warning: [OCEANBASE_SOURCE] uses non-SYS login (%s). "
            "Runtime will backfill SQL text through [%s]. If coverage is still low, use SYS "
            "or enable _enable_sql_audit_query_sql=true."
        ) % (source_user, SECTION_OCEANBASE_SOURCE_SYS)
    return (
        "QUERY_SQL visibility warning: [OCEANBASE_SOURCE] uses non-SYS login (%s). "
        "GV$OB_SQL_AUDIT.QUERY_SQL may be hidden on OB 4.2.5. Use SYS, configure [%s], "
        "or enable _enable_sql_audit_query_sql=true."
    ) % (source_user, SECTION_OCEANBASE_SOURCE_SYS)


def emit_prominent_runtime_warnings(config, mode):
    # type: (AppConfig, str) -> None
    messages = []
    visibility_warning = build_query_sql_visibility_warning(config)
    if visibility_warning and mode in (MODE_BATCH, MODE_SOURCE_REPORT):
        messages.append(visibility_warning)
    if mode == MODE_STREAM:
        messages.append(
            "Rolling Oracle-to-OceanBase monitor is active: new Oracle SQL and PL/SQL fingerprints "
            "will be replayed to OceanBase and report files will refresh in place."
        )
    for message in messages:
        LOG.warning("=" * 88)
        LOG.warning(message)
        LOG.warning("=" * 88)


def classify_replay_workload_type(sql_text):
    # type: (Any) -> str
    return "plsql" if is_plsql_statement(str(sql_text or "")) else "sql"


def build_workload_identity(row):
    # type: (Dict[str, Any]) -> str
    schema = str(row.get("schema") or "").strip().upper()
    sql_id = str(row.get("sql_id") or "").strip() or compute_sql_id(str(row.get("sql_text") or ""))
    normalized = normalize_sql_text(str(row.get("sql_text") or ""))
    return "%s|%s|%s" % (schema, sql_id, normalized)


def build_workload_event_id(row):
    # type: (Dict[str, Any]) -> str
    payload = "|".join(
        [
            build_workload_identity(row),
            str(row.get("captured_at") or ""),
        ]
    )
    return compute_sql_id(payload)


def _get_required_section(parser, section_name):
    # type: (configparser.ConfigParser, str) -> configparser.SectionProxy
    if not parser.has_section(section_name):
        raise ConfigError("config.ini missing [%s] section" % section_name)
    return parser[section_name]


def _get_required_value(section, section_name, key):
    # type: (configparser.SectionProxy, str, str) -> str
    value = (section.get(key) or "").strip()
    if not value:
        raise ConfigError("[%s] missing required key: %s" % (section_name, key))
    return value


def _load_ob_section(parser, section_name):
    # type: (configparser.ConfigParser, str) -> Dict[str, str]
    section = _get_required_section(parser, section_name)
    return {
        "executable": _get_required_value(section, section_name, "executable"),
        "host": _get_required_value(section, section_name, "host"),
        "port": _get_required_value(section, section_name, "port"),
        "user_string": _get_required_value(section, section_name, "user_string"),
        "password": _get_required_value(section, section_name, "password"),
    }


def _get_optional_int(section, key, default_value):
    # type: (configparser.SectionProxy, str, int) -> int
    raw = (section.get(key) or "").strip()
    if not raw:
        return default_value
    try:
        return int(raw)
    except ValueError:
        raise ConfigError("[%s] must be an integer" % key)


def _get_optional_float(section, key, default_value):
    # type: (configparser.SectionProxy, str, float) -> float
    raw = (section.get(key) or "").strip()
    if not raw:
        return default_value
    try:
        return float(raw)
    except ValueError:
        raise ConfigError("[%s] must be a number" % key)


def load_config(config_path, execution_mode=None):
    # type: (str, Optional[str]) -> AppConfig
    parser = configparser.ConfigParser(
        interpolation=None, inline_comment_prefixes=("#", ";")
    )
    read_files = parser.read(config_path, encoding="utf-8")
    if not read_files:
        raise ConfigError("Unable to read config file: %s" % config_path)

    settings_section = _get_required_section(parser, SECTION_SETTINGS)
    source_db_mode = (settings_section.get("source_db_mode") or SOURCE_DB_MODE_ORACLE).strip().lower()
    if source_db_mode not in (SOURCE_DB_MODE_ORACLE, SOURCE_DB_MODE_OCEANBASE):
        raise ConfigError("[SETTINGS] source_db_mode must be oracle or oceanbase")

    oracle_source = {}
    if parser.has_section(SECTION_ORACLE_SOURCE):
        oracle_section = _get_required_section(parser, SECTION_ORACLE_SOURCE)
        oracle_source = {
            "user": _get_required_value(oracle_section, SECTION_ORACLE_SOURCE, "user"),
            "password": _get_required_value(oracle_section, SECTION_ORACLE_SOURCE, "password"),
            "dsn": _get_required_value(oracle_section, SECTION_ORACLE_SOURCE, "dsn"),
        }
    elif source_db_mode != SOURCE_DB_MODE_OCEANBASE:
        raise ConfigError("config.ini missing [%s] section" % SECTION_ORACLE_SOURCE)
    oceanbase_source = {}
    if source_db_mode == SOURCE_DB_MODE_OCEANBASE:
        oceanbase_source = _load_ob_section(parser, SECTION_OCEANBASE_SOURCE)
    oceanbase_source_sys = {}
    if parser.has_section(SECTION_OCEANBASE_SOURCE_SYS):
        oceanbase_source_sys = _load_ob_section(parser, SECTION_OCEANBASE_SOURCE_SYS)
    oceanbase_target = {}
    requires_target = execution_mode != MODE_SOURCE_REPORT
    if parser.has_section(SECTION_OCEANBASE_TARGET):
        oceanbase_target = _load_ob_section(parser, SECTION_OCEANBASE_TARGET)
    elif requires_target:
        raise ConfigError("config.ini missing [%s] section" % SECTION_OCEANBASE_TARGET)

    source_schemas = normalize_schema_list(settings_section.get("source_schemas", ""))
    if not source_schemas:
        raise ConfigError("[SETTINGS] source_schemas must not be empty")

    settings = {
        "source_schemas": source_schemas,
        "source_db_mode": source_db_mode,
        "workloads_dir": (settings_section.get("workloads_dir") or DEFAULT_WORKLOADS_DIR).strip()
        or DEFAULT_WORKLOADS_DIR,
        "report_dir": (settings_section.get("report_dir") or DEFAULT_REPORT_DIR).strip()
        or DEFAULT_REPORT_DIR,
        "log_level": (settings_section.get("log_level") or "INFO").strip().upper() or "INFO",
        "top_n": _get_optional_int(settings_section, "top_n", DEFAULT_TOP_N),
        "capture_top_n": _get_optional_int(settings_section, "capture_top_n", DEFAULT_CAPTURE_TOP_N),
        "min_exec": _get_optional_int(settings_section, "min_exec", DEFAULT_MIN_EXEC),
        "hours": _get_optional_int(settings_section, "hours", DEFAULT_HOURS),
        "interval": _get_optional_int(settings_section, "interval", DEFAULT_INTERVAL),
        "rolling_report_interval": _get_optional_int(
            settings_section, "rolling_report_interval", DEFAULT_ROLLING_REPORT_INTERVAL
        ),
        "audit_poll_ms": _get_optional_int(
            settings_section, "audit_poll_ms", DEFAULT_AUDIT_POLL_MS
        ),
        "duration": _get_optional_int(settings_section, "duration", 0),
        "verify_results": str(settings_section.get("verify_results") or "false").strip().lower()
        in ("1", "true", "yes", "on"),
        "result_sample_limit": _get_optional_int(settings_section, "result_sample_limit", 10000),
        "plsql_profile": str(settings_section.get("plsql_profile") or "false").strip().lower()
        in ("1", "true", "yes", "on"),
        "plsql_profile_top_n": _get_optional_int(
            settings_section, "plsql_profile_top_n", DEFAULT_PLSQL_PROFILE_TOP_N
        ),
        "plsql_profile_source_context": _get_optional_int(
            settings_section,
            "plsql_profile_source_context",
            DEFAULT_PLSQL_PROFILE_SOURCE_CONTEXT,
        ),
        "obclient_timeout": _get_optional_int(
            settings_section, "obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT
        ),
        "ob_session_query_timeout_us": _get_optional_int(
            settings_section,
            "ob_session_query_timeout_us",
            DEFAULT_OB_SESSION_QUERY_TIMEOUT_US,
        ),
        "timeout_factor": _get_optional_float(
            settings_section, "timeout_factor", DEFAULT_TIMEOUT_FACTOR
        ),
        "slowdown_threshold": _get_optional_float(
            settings_section, "slowdown_threshold", DEFAULT_SLOWDOWN_THRESHOLD
        ),
        "wcr_path": (settings_section.get("wcr_path") or "").strip(),
        "ocp_base_url": (settings_section.get("ocp_base_url") or "").strip(),
        "ocp_authorization_env": (settings_section.get("ocp_authorization_env") or "").strip(),
        "ocp_username": (settings_section.get("ocp_username") or "").strip(),
        "ocp_password": (settings_section.get("ocp_password") or "").strip(),
        "ocp_password_env": (settings_section.get("ocp_password_env") or "").strip(),
        "ocp_cluster_id": (settings_section.get("ocp_cluster_id") or "").strip(),
        "ocp_tenant_id": (settings_section.get("ocp_tenant_id") or "").strip(),
        "ocp_cluster_name": (settings_section.get("ocp_cluster_name") or "").strip(),
        "ocp_tenant_name": (settings_section.get("ocp_tenant_name") or "").strip(),
        "ocp_verify_tls": str(settings_section.get("ocp_verify_tls") or "true").strip().lower()
        in ("1", "true", "yes", "on"),
        "ocp_window_minutes": _get_optional_int(settings_section, "ocp_window_minutes", 15),
        "ocp_query_limit": _get_optional_int(settings_section, "ocp_query_limit", 20),
        "ocp_ash_url_template": (settings_section.get("ocp_ash_url_template") or "").strip(),
        "ocp_qpm_url_template": (settings_section.get("ocp_qpm_url_template") or "").strip(),
        "ocp_auth_token_env": (settings_section.get("ocp_auth_token_env") or "").strip(),
        "ocp_timeout": _get_optional_int(settings_section, "ocp_timeout", DEFAULT_OCP_TIMEOUT),
        "obdiag_executable": (settings_section.get("obdiag_executable") or "").strip(),
        "obdiag_timeout": _get_optional_int(settings_section, "obdiag_timeout", DEFAULT_OBDIAG_TIMEOUT),
        "obdiag_extra_args": (settings_section.get("obdiag_extra_args") or "").strip(),
        "source_actor_fields": normalize_csv_list(
            settings_section.get(
                "source_actor_fields", ",".join(DEFAULT_SOURCE_ACTOR_FIELDS)
            )
        )
        or list(DEFAULT_SOURCE_ACTOR_FIELDS),
    }

    return AppConfig(
        oracle_source=oracle_source,
        oceanbase_source=oceanbase_source,
        oceanbase_target=oceanbase_target,
        settings=settings,
        config_path=str(config_path),
        oceanbase_source_sys=oceanbase_source_sys,
    )


def parse_oracle_dsn(dsn):
    # type: (str) -> Tuple[str, str, str]
    try:
        host_port, service_name = str(dsn).split("/", 1)
        host, port = host_port.split(":", 1)
    except ValueError:
        raise ConfigError("Oracle DSN must use host:port/service_name format")
    host = host.strip()
    port = port.strip()
    service_name = service_name.strip()
    if not host or not port or not service_name:
        raise ConfigError("Oracle DSN must use host:port/service_name format")
    return host, port, service_name


def _coerce_jsonable(value):
    # type: (Any) -> Any
    if value is None or isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    if hasattr(value, "read") and callable(value.read):
        try:
            return value.read()
        except Exception:
            return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def generate_run_id(now=None):
    # type: (Optional[datetime]) -> str
    dt = now or datetime.now()
    return dt.strftime("%Y%m%d_%H%M%S")


def utc_now_iso():
    # type: () -> str
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_artifact_path(kind, run_id, root_dir=DEFAULT_WORKLOADS_DIR):
    # type: (str, str, Union[str, Path]) -> Path
    if kind not in ARTIFACT_SPECS:
        raise KeyError("Unknown artifact kind: %s" % kind)
    prefix, suffix = ARTIFACT_SPECS[kind]
    return Path(root_dir) / ("%s_%s%s" % (prefix, run_id, suffix))


def write_text(path, content):
    # type: (Union[str, Path], str) -> None
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("w", encoding="utf-8") as handle:
        handle.write(content)


def read_jsonl(path):
    # type: (Union[str, Path]) -> List[Dict[str, Any]]
    rows = []
    file_path = Path(path)
    if not file_path.exists():
        return rows
    with file_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def append_jsonl(path, rows):
    # type: (Union[str, Path], Union[Dict[str, Any], Iterable[Dict[str, Any]]]) -> None
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(rows, dict):
        iterable = [rows]
    else:
        iterable = list(rows)
    with file_path.open("a", encoding="utf-8") as handle:
        for row in iterable:
            handle.write(json.dumps(row, ensure_ascii=True, sort_keys=True))
            handle.write("\n")


def write_json(path, payload):
    # type: (Union[str, Path], Dict[str, Any]) -> None
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, indent=2, sort_keys=True)
        handle.write("\n")


def normalize_sql_text(sql_text):
    # type: (str) -> str
    collapsed = re.sub(r"\s+", " ", str(sql_text or "")).strip()
    return collapsed.upper()


def is_missing_sql_text(sql_text):
    # type: (Any) -> bool
    normalized = normalize_sql_text(sql_text)
    return normalized in ("", "NULL", "NONE")


def build_sql_preview(sql_text, limit=160):
    # type: (Any, int) -> str
    if is_missing_sql_text(sql_text):
        return "<SQL text unavailable>"
    collapsed = re.sub(r"\s+", " ", str(sql_text or "")).strip()
    if len(collapsed) <= int(limit):
        return collapsed
    return collapsed[: max(1, int(limit) - 3)].rstrip() + "..."


def is_internal_perf_comparator_source_sql(sql_text):
    # type: (Any) -> bool
    normalized = normalize_sql_text(sql_text)
    if "PERF_COMPARATOR_SOURCE_" in normalized:
        return True
    patterns = (
        "SELECT REQUEST_ID, TRACE_ID, SQL_ID, ELAPSED_TIME",
        "SELECT NVL(MAX(REQUEST_ID)",
        "SELECT SQL_ID, PLAN_TYPE, REPLACE(REPLACE(REPLACE(QUERY_SQL",
        "SELECT SQL_ID, REPLACE(REPLACE(REPLACE(QUERY_SQL",
    )
    if "FROM GV$OB_SQL_AUDIT" in normalized and any(
        pattern in normalized for pattern in patterns[:2]
    ):
        return True
    if "FROM GV$OB_SQLSTAT" in normalized and any(
        pattern in normalized for pattern in patterns[2:]
    ):
        return True
    if "FROM GV$OB_PLAN_CACHE_PLAN_STAT" in normalized and patterns[3] in normalized:
        return True
    return False


def compute_sql_id(sql_text):
    # type: (str) -> str
    return hashlib.sha1(normalize_sql_text(sql_text).encode("utf-8")).hexdigest()[:16]


def _split_object_name(raw_name):
    # type: (str) -> Tuple[Optional[str], str]
    text = str(raw_name or "").strip().strip(",")
    if not text:
        return None, ""
    text = text.strip('"')
    if "." not in text:
        return None, text
    schema_name, object_name = text.split(".", 1)
    return schema_name.strip('"'), object_name.strip('"')


def extract_table_references(sql_text):
    # type: (str) -> List[Tuple[Optional[str], str]]
    text = str(sql_text or "")
    text = re.sub(r"/\*.*?\*/", " ", text, flags=re.S)
    text = re.sub(r"--.*?$", " ", text, flags=re.M)
    pattern = re.compile(
        r"\b(?:FROM|JOIN|UPDATE|INTO|MERGE\s+INTO)\s+((?:\"?[A-Za-z0-9_$#]+\"?\.)?\"?[A-Za-z0-9_$#]+\"?)",
        re.I,
    )
    seen = set()
    tables = []
    for match in pattern.finditer(text):
        schema_name, object_name = _split_object_name(match.group(1))
        if not object_name:
            continue
        key = (str(schema_name or "").upper(), object_name.upper())
        if key in seen:
            continue
        seen.add(key)
        tables.append((schema_name, object_name))
    return tables


def split_sql_text(sql_text):
    # type: (str) -> List[str]
    statements = []
    buffer_chars = []
    in_single = False
    in_double = False
    in_dollar = False
    for raw_line in str(sql_text or "").splitlines():
        stripped = raw_line.strip()
        if stripped == "$$" and not in_single and not in_double:
            if in_dollar:
                statement = "".join(buffer_chars).strip()
                if statement:
                    statements.append(statement)
                buffer_chars = []
                in_dollar = False
            else:
                pending = "".join(buffer_chars).strip()
                if pending:
                    statements.append(pending)
                buffer_chars = []
                in_dollar = True
            continue

        if in_dollar:
            buffer_chars.append(raw_line)
            buffer_chars.append("\n")
            continue

        line = raw_line + "\n"
        idx = 0
        while idx < len(line):
            ch = line[idx]
            prev = line[idx - 1] if idx > 0 else ""
            if ch == "'" and not in_double and prev != "\\":
                in_single = not in_single
            elif ch == '"' and not in_single and prev != "\\":
                in_double = not in_double
            elif ch == ";" and not in_single and not in_double:
                statement = "".join(buffer_chars).strip()
                if statement:
                    statements.append(statement)
                buffer_chars = []
                idx += 1
                continue
            buffer_chars.append(ch)
            idx += 1

    tail = "".join(buffer_chars).strip()
    if tail:
        statements.append(tail)
    return statements


def parse_ob_audit_rows(stdout_text, default_schema, captured_at=None):
    # type: (str, str, Optional[str]) -> List[Dict[str, Any]]
    rows = []
    stamped_at = captured_at or utc_now_iso()
    for line in (stdout_text or "").splitlines():
        fields = line.split("\t")
        if len(fields) < 15:
            continue
        format_kind = "legacy"
        if len(fields) >= 27:
            format_kind = "source425ctx"
        elif len(fields) >= 21:
            format_kind = "source425"
        elif len(fields) >= 19:
            format_kind = "rich"
        is_source425 = format_kind in ("source425", "source425ctx")
        retry_idx = 13 if is_source425 else (12 if format_kind == "rich" else None)
        table_scan_idx = 12 if is_source425 else None
        row_cache_hit_idx = 14 if is_source425 else None
        block_cache_hit_idx = 15 if is_source425 else None
        memstore_idx = 16 if is_source425 else (13 if format_kind == "rich" else None)
        ssstore_idx = 17 if is_source425 else (14 if format_kind == "rich" else None)
        bloom_idx = 15 if format_kind == "rich" else None
        logical_idx = 18 if is_source425 else (16 if format_kind == "rich" else 12)
        physical_idx = 19 if is_source425 else (17 if format_kind == "rich" else 13)
        sql_text_idx = 20 if is_source425 else (18 if format_kind == "rich" else 14)
        tenant_name_idx = 21 if format_kind == "source425ctx" else None
        db_name_idx = 22 if format_kind == "source425ctx" else None
        user_name_idx = 23 if format_kind == "source425ctx" else None
        user_client_ip_idx = 24 if format_kind == "source425ctx" else None
        client_ip_idx = 25 if format_kind == "source425ctx" else None
        ret_code_idx = 26 if format_kind == "source425ctx" else None
        sql_id = fields[2] or compute_sql_id(fields[sql_text_idx])
        sql_text = fields[sql_text_idx]
        rows.append(
            {
                "sql_id": sql_id,
                "sql_text": sql_text,
                "sql_text_normalized": normalize_sql_text(sql_text),
                "bind_vars": {},
                "schema": default_schema,
                "source": "ob_sql_audit",
                "captured_at": stamped_at,
                "baseline_source_mode": SOURCE_DB_MODE_OCEANBASE,
                "baseline_avg_elapsed_us": _safe_float(fields[3]),
                "baseline_avg_logical_reads": _safe_float(fields[logical_idx]),
                "oracle_executions": 1,
                "oracle_avg_elapsed_us": _safe_float(fields[3]),
                "oracle_avg_cpu_us": None,
                "oracle_avg_logical_reads": _safe_float(fields[logical_idx]),
                "oracle_avg_physical_reads": _safe_float(fields[physical_idx]),
                "oracle_plan_hash": fields[9] or None,
                "oracle_plan_rows": [],
                "source_ob_request_id": fields[0],
                "source_ob_trace_id": fields[1],
                "source_ob_queue_time_us": _safe_float(fields[4]),
                "source_ob_get_plan_time_us": _safe_float(fields[5]),
                "source_ob_execute_time_us": _safe_float(fields[6]),
                "source_ob_net_time_us": _safe_float(fields[7]),
                "source_ob_net_wait_time_us": _safe_float(fields[8]),
                "source_ob_table_scan": _safe_float(fields[table_scan_idx]) if table_scan_idx is not None else None,
                "source_ob_plan_type_raw": fields[9],
                "source_ob_plan_type": PLAN_TYPE_NAMES.get(str(fields[9]), str(fields[9])),
                "source_ob_is_hit_plan": fields[10],
                "source_ob_is_executor_rpc": fields[11],
                "source_ob_retry_cnt": _safe_float(fields[retry_idx]) if retry_idx is not None else None,
                "source_ob_row_cache_hit": _safe_float(fields[row_cache_hit_idx]) if row_cache_hit_idx is not None else None,
                "source_ob_block_cache_hit": _safe_float(fields[block_cache_hit_idx]) if block_cache_hit_idx is not None else None,
                "source_ob_memstore_read_rows": _safe_float(fields[memstore_idx]) if memstore_idx is not None else None,
                "source_ob_ssstore_read_rows": _safe_float(fields[ssstore_idx]) if ssstore_idx is not None else None,
                "source_ob_bloom_filter_filtered": _safe_float(fields[bloom_idx]) if bloom_idx is not None else None,
                "source_ob_logical_reads": _safe_float(fields[logical_idx]),
                "source_ob_physical_reads": _safe_float(fields[physical_idx]),
                "source_tenant_name": fields[tenant_name_idx] if tenant_name_idx is not None else "",
                "source_db_name": fields[db_name_idx] if db_name_idx is not None else "",
                "source_user_name": fields[user_name_idx] if user_name_idx is not None else "",
                "source_user_client_ip": fields[user_client_ip_idx] if user_client_ip_idx is not None else "",
                "source_client_ip": fields[client_ip_idx] if client_ip_idx is not None else "",
                "source_ret_code": _safe_float(fields[ret_code_idx]) if ret_code_idx is not None else None,
            }
        )
    return rows


def classify_source_workload_type(sql_text):
    # type: (Any) -> str
    return "plsql" if is_plsql_statement(str(sql_text or "")) else "sql"


def get_source_actor_fields(config_or_settings):
    # type: (Any) -> List[str]
    if isinstance(config_or_settings, dict):
        settings = config_or_settings
    else:
        settings = getattr(config_or_settings, "settings", {}) or {}
    actor_fields = normalize_csv_list(settings.get("source_actor_fields"))
    return actor_fields or list(DEFAULT_SOURCE_ACTOR_FIELDS)


def build_source_fallback_actor_key(row):
    # type: (Dict[str, Any]) -> str
    fallback_values = [
        ("schema", str(row.get("schema") or "").strip()),
        ("sql_id", str(row.get("sql_id") or "").strip()),
    ]
    parts = ["%s=%s" % (name, value) for name, value in fallback_values if value]
    return " | ".join(parts) or "unattributed"


def has_source_actor_attribution(row, actor_fields):
    # type: (Dict[str, Any], Sequence[str]) -> bool
    for field_name in actor_fields:
        key = "source_%s" % str(field_name or "").strip().lower()
        value = str(row.get(key) or row.get(field_name) or "").strip()
        if value:
            return True
    return False


def build_source_actor_key(row, actor_fields, allow_fallback=True):
    # type: (Dict[str, Any], Sequence[str], bool) -> str
    parts = []
    for field_name in actor_fields:
        key = "source_%s" % str(field_name or "").strip().lower()
        value = str(row.get(key) or row.get(field_name) or "").strip()
        if value:
            parts.append("%s=%s" % (field_name, value))
    if parts:
        return " | ".join(parts)
    if not allow_fallback:
        return ""
    return build_source_fallback_actor_key(row)


def summarize_source_attribution(row):
    # type: (Dict[str, Any]) -> str
    quality = str(row.get("source_attribution_quality") or "unattributed")
    direct = int(row.get("source_direct_sample_count") or 0)
    fallback = int(row.get("source_fallback_sample_count") or 0)
    if quality == "mixed":
        return "mixed(direct=%d,fallback=%d)" % (direct, fallback)
    if quality == "direct":
        return "direct(%d)" % direct
    if quality == "fallback":
        return "fallback(%d)" % fallback
    return quality


def summarize_source_likely_cause(row):
    # type: (Dict[str, Any]) -> str
    plsql_diagnosis = summarize_plsql_profile_diagnosis(row)
    if plsql_diagnosis != "n/a":
        return plsql_diagnosis
    recommendations = row.get("recommendations") or []
    if recommendations:
        rule_ids = [str(item.get("rule_id") or "").strip() for item in recommendations if str(item.get("rule_id") or "").strip()]
        if rule_ids:
            return ",".join(rule_ids[:3])
    plan_risk = summarize_plan_diff_signals(row)
    if plan_risk != "n/a":
        return plan_risk
    net_ratio = row.get("net_ratio")
    if net_ratio is not None and float(net_ratio or 0.0) > 0.6:
        return "network_heavy"
    return "n/a"


def compute_source_actor_summaries(rows):
    # type: (List[Dict[str, Any]]) -> List[Dict[str, Any]]
    grouped = {}
    direct_available = any(row.get("source_direct_actor_summaries") for row in rows)
    for row in rows:
        actor_summaries = (
            row.get("source_direct_actor_summaries")
            if direct_available
            else (row.get("source_direct_actor_summaries") or row.get("source_fallback_actor_summaries"))
        ) or []
        actor_sample_total = (
            int(row.get("source_direct_sample_count") or 0)
            if direct_available
            else int(row.get("source_direct_sample_count") or row.get("source_fallback_sample_count") or 0)
        )
        if not actor_summaries:
            continue
        denominator = max(1, actor_sample_total)
        total_elapsed_us = float(row.get("source_total_elapsed_us") or 0.0)
        for actor_summary in actor_summaries:
            actor_key = str(actor_summary.get("actor") or "").strip() or "unattributed"
            sample_count = int(actor_summary.get("count") or 0)
            if sample_count <= 0:
                continue
            entry = grouped.get(actor_key)
            if entry is None:
                entry = {
                    "actor": actor_key,
                    "statement_count": 0,
                    "sql_count": 0,
                    "plsql_count": 0,
                    "sample_count": 0,
                    "total_elapsed_us": 0.0,
                    "max_avg_elapsed_us": 0.0,
                    "attribution_scope": "direct" if direct_available else "fallback",
                }
                grouped[actor_key] = entry
            entry["statement_count"] += 1
            entry["sample_count"] += sample_count
            entry["total_elapsed_us"] += total_elapsed_us * (float(sample_count) / float(denominator))
            entry["max_avg_elapsed_us"] = max(entry["max_avg_elapsed_us"], float(row.get("ob_elapsed_us") or 0.0))
            if str(row.get("source_workload_type") or "sql") == "plsql":
                entry["plsql_count"] += 1
            else:
                entry["sql_count"] += 1
    return sorted(
        grouped.values(),
        key=lambda item: (-float(item.get("total_elapsed_us") or 0.0), -int(item.get("sample_count") or 0), str(item.get("actor") or "")),
    )


def maybe_refresh_source_report(config, workload_path, run_id, last_refresh_at, force=False):
    # type: (AppConfig, Union[str, Path], str, float, bool) -> float
    refresh_interval = int(config.settings.get("rolling_report_interval", DEFAULT_ROLLING_REPORT_INTERVAL) or 0)
    if refresh_interval <= 0:
        return float(last_refresh_at or 0.0)
    now = time.time()
    if not force and now - float(last_refresh_at or 0.0) < refresh_interval:
        return float(last_refresh_at or 0.0)
    if not Path(workload_path).exists():
        return float(last_refresh_at or 0.0)
    rolling_config = clone_app_config(config, settings_updates={"_rolling_source_report": True})
    generate_report_from_source_workload(rolling_config, workload_path, run_id)
    LOG.info("Rolling source report refreshed: run_id=%s", run_id)
    return now


def get_source_sql_lookup_ob_cfg(config):
    # type: (AppConfig) -> Dict[str, str]
    if config.oceanbase_source_sys:
        return config.oceanbase_source_sys
    return config.oceanbase_source


def _build_source_sql_text_lookup_query(view_name, sql_ids):
    # type: (str, Sequence[str]) -> str
    escaped_ids = []
    for sql_id in sql_ids:
        raw_sql_id = str(sql_id or "").strip()
        if not raw_sql_id:
            continue
        escaped_ids.append("'%s'" % raw_sql_id.replace("'", "''"))
    if not escaped_ids:
        return ""
    return """
        SELECT /* perf_comparator_source_sql_lookup */
          SQL_ID,
          REPLACE(REPLACE(REPLACE(QUERY_SQL, CHR(10), ' '), CHR(13), ' '), CHR(9), ' ')
        FROM {view_name}
        WHERE QUERY_SQL IS NOT NULL
          AND SQL_ID IN ({sql_ids})
        ORDER BY SQL_ID
    """.format(view_name=view_name, sql_ids=", ".join(escaped_ids))


def _parse_source_sql_text_lookup_rows(stdout_text):
    # type: (str) -> Dict[str, str]
    mapping = {}
    for line in (stdout_text or "").splitlines():
        fields = line.split("\t", 1)
        if len(fields) < 2:
            continue
        sql_id = str(fields[0] or "").strip()
        sql_text = str(fields[1] or "").strip()
        if not sql_id or is_missing_sql_text(sql_text) or sql_id in mapping:
            continue
        mapping[sql_id] = sql_text
    return mapping


def lookup_source_sql_texts(config, sql_ids):
    # type: (AppConfig, Sequence[str]) -> Dict[str, str]
    ob_cfg = get_source_sql_lookup_ob_cfg(config)
    if not ob_cfg:
        return {}
    pending_ids = []
    seen = set()
    for sql_id in sql_ids:
        raw_sql_id = str(sql_id or "").strip()
        if not raw_sql_id or raw_sql_id in seen:
            continue
        seen.add(raw_sql_id)
        pending_ids.append(raw_sql_id)
    if not pending_ids:
        return {}

    resolved = {}  # type: Dict[str, str]
    view_names = [
        "GV$OB_SQLSTAT",
        "GV$OB_PLAN_CACHE_PLAN_STAT",
        "GV$OB_SQL_AUDIT",
    ]
    batch_size = 50
    for start_idx in range(0, len(pending_ids), batch_size):
        batch_ids = pending_ids[start_idx : start_idx + batch_size]
        unresolved = list(batch_ids)
        for view_name in view_names:
            if not unresolved:
                break
            query = _build_source_sql_text_lookup_query(view_name, unresolved)
            if not query:
                continue
            ok, stdout, _ = obclient_run_sql(
                ob_cfg,
                query,
                timeout=config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT),
                session_query_timeout_us=config.settings.get(
                    "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
                ),
            )
            if not ok or not stdout.strip():
                continue
            resolved.update(_parse_source_sql_text_lookup_rows(stdout))
            unresolved = [sql_id for sql_id in unresolved if sql_id not in resolved]
    return resolved


def lookup_sql_texts_via_ocp_native(config, sql_ids):
    # type: (AppConfig, Sequence[str]) -> Dict[str, str]
    capability = _probe_ocp_capability(config)
    if capability.get("mode") != "native" or capability.get("status") != "ready":
        return {}
    resolved = {}
    headers = _build_ocp_headers(config)
    timeout_seconds = int(config.settings.get("ocp_timeout", DEFAULT_OCP_TIMEOUT))
    report_dir = config.settings.get("report_dir") or config.settings.get("workloads_dir") or "."
    run_id = str(config.settings.get("_current_run_id") or "ocp_lookup")
    for sql_id in sql_ids:
        sql_id = str(sql_id or "").strip()
        if not sql_id:
            continue
        url = _build_ocp_native_sql_text_url(config, sql_id)
        request = urllib.request.Request(url, headers=headers)
        try:
            response = _open_ocp_request(config, request, timeout_seconds)
            body = response.read().decode("utf-8", errors="ignore")
        except Exception:
            continue
        artifact_path = _build_external_diagnostic_path(report_dir, run_id, sql_id, "sql_text_lookup", ".json")
        write_text(artifact_path, body)
        try:
            payload = json.loads(body)
        except Exception:
            payload = {}
        sql_text = ""
        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, dict):
                sql_text = str(_first_non_empty(data, ("sqlText", "sql_text", "text")) or "")
            if not sql_text:
                sql_text = str(_first_non_empty(payload, ("sqlText", "sql_text", "text")) or "")
        if str(sql_text or "").strip():
            resolved[sql_id] = sql_text
    return resolved


def lookup_sql_texts_via_ocp_template(config, sql_ids):
    # type: (AppConfig, Sequence[str]) -> Dict[str, str]
    template = str(config.settings.get("ocp_qpm_url_template") or config.settings.get("ocp_ash_url_template") or "").strip()
    if not template:
        return {}
    resolved = {}
    timeout_seconds = int(config.settings.get("ocp_timeout", DEFAULT_OCP_TIMEOUT))
    report_dir = config.settings.get("report_dir") or config.settings.get("workloads_dir") or "."
    run_id = str(config.settings.get("_current_run_id") or "ocp_lookup")
    headers = _build_ocp_headers(config)
    for sql_id in sql_ids:
        sql_id = str(sql_id or "").strip()
        if not sql_id:
            continue
        url = _format_external_template(template, {"sql_id": sql_id, "run_id": run_id})
        request = urllib.request.Request(url, headers=headers)
        try:
            response = _open_ocp_request(config, request, timeout_seconds)
            body = response.read().decode("utf-8", errors="ignore")
        except Exception:
            continue
        artifact_path = _build_external_diagnostic_path(report_dir, run_id, sql_id, "sql_template_lookup", ".txt")
        write_text(artifact_path, body)
        if str(body or "").strip():
            resolved[sql_id] = str(body).strip()
    return resolved


def compute_sql_text_source_distribution(rows):
    # type: (Sequence[Dict[str, Any]]) -> Dict[str, int]
    counts = {}  # type: Dict[str, int]
    for row in rows:
        source = str(
            row.get("source_sql_text_source")
            or row.get("source_sql_text_status")
            or ("missing" if is_missing_sql_text(row.get("sql_text")) else "captured")
        ).strip() or "unknown"
        counts[source] = counts.get(source, 0) + 1
    return counts


def backfill_source_workload_sql_texts(config, workload_rows):
    # type: (AppConfig, List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]
    lookup_cfg = get_source_sql_lookup_ob_cfg(config)
    missing_sql_ids = []
    for row in workload_rows:
        if is_missing_sql_text(row.get("sql_text")):
            sql_id = str(row.get("sql_id") or "").strip()
            if sql_id:
                missing_sql_ids.append(sql_id)
    lookup_map = lookup_source_sql_texts(config, missing_sql_ids) if missing_sql_ids else {}
    remaining_sql_ids = [sql_id for sql_id in missing_sql_ids if sql_id not in lookup_map]
    ocp_native_map = lookup_sql_texts_via_ocp_native(config, remaining_sql_ids) if remaining_sql_ids else {}
    remaining_sql_ids = [sql_id for sql_id in remaining_sql_ids if sql_id not in ocp_native_map]
    ocp_template_map = lookup_sql_texts_via_ocp_template(config, remaining_sql_ids) if remaining_sql_ids else {}
    stats = {
        "captured": 0,
        "backfilled": 0,
        "backfilled_via_local": 0,
        "backfilled_via_ocp_native": 0,
        "backfilled_via_ocp_template": 0,
        "missing": 0,
        "lookup_user": str(lookup_cfg.get("user_string") or ""),
        "using_source_sys": bool(config.oceanbase_source_sys),
    }
    enriched_rows = []
    for row in workload_rows:
        updated = dict(row)
        sql_id = str(updated.get("sql_id") or "").strip()
        raw_sql_text = updated.get("sql_text")
        if is_missing_sql_text(raw_sql_text):
            recovered_sql_text = lookup_map.get(sql_id, "")
            recovered_source = ""
            if recovered_sql_text:
                recovered_source = "source_sys"
            elif sql_id in ocp_native_map:
                recovered_sql_text = ocp_native_map.get(sql_id, "")
                recovered_source = "ocp_native"
            elif sql_id in ocp_template_map:
                recovered_sql_text = ocp_template_map.get(sql_id, "")
                recovered_source = "ocp_template"
            if recovered_sql_text:
                updated["sql_text"] = recovered_sql_text
                updated["sql_text_normalized"] = normalize_sql_text(recovered_sql_text)
                updated["source_sql_text_status"] = "backfilled"
                updated["source_sql_text_source"] = recovered_source or "source_sys"
                stats["backfilled"] += 1
                if recovered_source == "ocp_native":
                    stats["backfilled_via_ocp_native"] += 1
                elif recovered_source == "ocp_template":
                    stats["backfilled_via_ocp_template"] += 1
                else:
                    stats["backfilled_via_local"] += 1
            else:
                updated["sql_text"] = ""
                updated["sql_text_normalized"] = ""
                updated["source_sql_text_status"] = "missing"
                updated["source_sql_text_source"] = "missing"
                stats["missing"] += 1
        else:
            sql_text = str(raw_sql_text)
            updated["sql_text"] = sql_text
            updated["sql_text_normalized"] = (
                updated.get("sql_text_normalized") or normalize_sql_text(sql_text)
            )
            updated["source_sql_text_status"] = "captured"
            updated["source_sql_text_source"] = (
                str(updated.get("source_sql_text_source") or "").strip() or "captured"
            )
            stats["captured"] += 1
        enriched_rows.append(updated)
    return enriched_rows, stats


def _parse_source_sqlstat_snapshot_rows(stdout_text):
    # type: (str) -> Dict[str, Dict[str, Any]]
    snapshot = {}
    for line in (stdout_text or "").splitlines():
        fields = line.split("\t")
        if len(fields) < 14:
            continue
        sql_id = str(fields[0] or "").strip()
        if not sql_id:
            continue
        snapshot[sql_id] = {
            "sql_id": sql_id,
            "plan_type": fields[1],
            "query_sql": fields[2],
            "executions_total": _safe_float(fields[3]) or 0.0,
            "elapsed_time_total": _safe_float(fields[4]) or 0.0,
            "buffer_gets_total": _safe_float(fields[5]) or 0.0,
            "disk_reads_total": _safe_float(fields[6]) or 0.0,
            "memstore_read_rows_total": _safe_float(fields[7]) or 0.0,
            "minor_ssstore_read_rows_total": _safe_float(fields[8]) or 0.0,
            "major_ssstore_read_rows_total": _safe_float(fields[9]) or 0.0,
            "rpc_total": _safe_float(fields[10]) or 0.0,
            "retry_total": _safe_float(fields[11]) or 0.0,
            "plan_cache_hit_total": _safe_float(fields[12]) or 0.0,
            "plan_hash": fields[13] or None,
        }
    return snapshot


def collect_source_sqlstat_snapshot(config):
    # type: (AppConfig) -> Dict[str, Dict[str, Any]]
    query = """
        SELECT /* perf_comparator_source_sqlstat_snapshot */
          SQL_ID,
          PLAN_TYPE,
          REPLACE(REPLACE(REPLACE(QUERY_SQL, CHR(10), ' '), CHR(13), ' '), CHR(9), ' '),
          EXECUTIONS_TOTAL,
          ELAPSED_TIME_TOTAL,
          BUFFER_GETS_TOTAL,
          DISK_READS_TOTAL,
          MEMSTORE_READ_ROWS_TOTAL,
          MINOR_SSSTORE_READ_ROWS_TOTAL,
          MAJOR_SSSTORE_READ_ROWS_TOTAL,
          RPC_TOTAL,
          RETRY_TOTAL,
          PLAN_CACHE_HIT_TOTAL,
          PLAN_HASH
        FROM GV$OB_SQLSTAT
    """
    ok, stdout, _ = _obclient_run_sql_on_source(
        config,
        query,
        timeout=config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT),
        session_query_timeout_us=config.settings.get(
            "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
        ),
    )
    if not ok or not stdout.strip():
        return {}
    return _parse_source_sqlstat_snapshot_rows(stdout)


def build_source_sqlstat_delta_rows(
    start_snapshot,
    end_snapshot,
    captured_sql_ids,
    default_schema,
    captured_at,
):
    # type: (Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], Iterable[str], str, str) -> List[Dict[str, Any]]
    captured_sql_id_set = set(str(sql_id) for sql_id in (captured_sql_ids or []) if sql_id)
    rows = []
    for sql_id, end_entry in end_snapshot.items():
        if sql_id in captured_sql_id_set:
            continue
        start_entry = start_snapshot.get(sql_id, {})
        executions_delta = max(
            0.0,
            float(end_entry.get("executions_total") or 0.0)
            - float(start_entry.get("executions_total") or 0.0),
        )
        elapsed_delta = max(
            0.0,
            float(end_entry.get("elapsed_time_total") or 0.0)
            - float(start_entry.get("elapsed_time_total") or 0.0),
        )
        if executions_delta <= 0.0 or elapsed_delta <= 0.0:
            continue
        logical_delta = max(
            0.0,
            float(end_entry.get("buffer_gets_total") or 0.0)
            - float(start_entry.get("buffer_gets_total") or 0.0),
        )
        physical_delta = max(
            0.0,
            float(end_entry.get("disk_reads_total") or 0.0)
            - float(start_entry.get("disk_reads_total") or 0.0),
        )
        memstore_delta = max(
            0.0,
            float(end_entry.get("memstore_read_rows_total") or 0.0)
            - float(start_entry.get("memstore_read_rows_total") or 0.0),
        )
        minor_delta = max(
            0.0,
            float(end_entry.get("minor_ssstore_read_rows_total") or 0.0)
            - float(start_entry.get("minor_ssstore_read_rows_total") or 0.0),
        )
        major_delta = max(
            0.0,
            float(end_entry.get("major_ssstore_read_rows_total") or 0.0)
            - float(start_entry.get("major_ssstore_read_rows_total") or 0.0),
        )
        rpc_delta = max(
            0.0,
            float(end_entry.get("rpc_total") or 0.0)
            - float(start_entry.get("rpc_total") or 0.0),
        )
        retry_delta = max(
            0.0,
            float(end_entry.get("retry_total") or 0.0)
            - float(start_entry.get("retry_total") or 0.0),
        )
        plan_hit_delta = max(
            0.0,
            float(end_entry.get("plan_cache_hit_total") or 0.0)
            - float(start_entry.get("plan_cache_hit_total") or 0.0),
        )
        avg_elapsed_us = elapsed_delta / executions_delta
        avg_logical_reads = logical_delta / executions_delta
        avg_physical_reads = physical_delta / executions_delta
        avg_memstore_reads = memstore_delta / executions_delta
        avg_ssstore_reads = (minor_delta + major_delta) / executions_delta
        avg_retry_cnt = retry_delta / executions_delta
        rows.append(
            {
                "sql_id": sql_id,
                "sql_text": end_entry.get("query_sql") or "",
                "sql_text_normalized": normalize_sql_text(end_entry.get("query_sql") or ""),
                "bind_vars": {},
                "schema": default_schema,
                "source": "ob_sqlstat_snapshot",
                "captured_at": captured_at,
                "baseline_source_mode": SOURCE_DB_MODE_OCEANBASE,
                "baseline_avg_elapsed_us": avg_elapsed_us,
                "baseline_avg_logical_reads": avg_logical_reads,
                "oracle_executions": int(executions_delta),
                "oracle_avg_elapsed_us": avg_elapsed_us,
                "oracle_avg_logical_reads": avg_logical_reads,
                "oracle_avg_physical_reads": avg_physical_reads,
                "oracle_plan_hash": end_entry.get("plan_hash"),
                "oracle_plan_rows": [],
                "source_execution_count": int(executions_delta),
                "source_total_elapsed_us": elapsed_delta,
                "source_ob_request_id": None,
                "source_ob_trace_id": None,
                "source_ob_queue_time_us": 0.0,
                "source_ob_get_plan_time_us": 0.0,
                "source_ob_execute_time_us": avg_elapsed_us,
                "source_ob_net_time_us": 0.0,
                "source_ob_net_wait_time_us": 0.0,
                "source_ob_plan_type_raw": end_entry.get("plan_type"),
                "source_ob_plan_type": PLAN_TYPE_NAMES.get(
                    str(end_entry.get("plan_type") or ""), str(end_entry.get("plan_type") or "")
                ),
                "source_ob_is_hit_plan": "1" if plan_hit_delta > 0.0 else "0",
                "source_ob_is_executor_rpc": "1" if rpc_delta > 0.0 else "0",
                "source_ob_retry_cnt": avg_retry_cnt,
                "source_ob_memstore_read_rows": avg_memstore_reads,
                "source_ob_ssstore_read_rows": avg_ssstore_reads,
                "source_ob_bloom_filter_filtered": 0.0,
                "source_ob_logical_reads": avg_logical_reads,
                "source_ob_physical_reads": avg_physical_reads,
            }
        )
    return rows


def _parse_source_plan_cache_recent_rows(stdout_text):
    # type: (str) -> List[Dict[str, Any]]
    rows = []
    for line in (stdout_text or "").splitlines():
        fields = line.split("\t")
        if len(fields) < 10:
            continue
        sql_id = str(fields[0] or "").strip()
        if not sql_id:
            continue
        rows.append(
            {
                "sql_id": sql_id,
                "query_sql": fields[1],
                "avg_exe_usec": _safe_float(fields[2]) or 0.0,
                "executions": _safe_float(fields[3]) or 0.0,
                "elapsed_time": _safe_float(fields[4]) or 0.0,
                "buffer_gets": _safe_float(fields[5]) or 0.0,
                "disk_reads": _safe_float(fields[6]) or 0.0,
                "hit_count": _safe_float(fields[7]) or 0.0,
                "type": fields[8],
                "table_scan": _safe_float(fields[9]) or 0.0,
            }
        )
    return rows


def collect_source_plan_cache_recent_rows(config, window_start):
    # type: (AppConfig, datetime) -> List[Dict[str, Any]]
    started_at = window_start.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    query = """
        SELECT /* perf_comparator_source_plan_cache_recent */
          SQL_ID,
          REPLACE(REPLACE(REPLACE(QUERY_SQL, CHR(10), ' '), CHR(13), ' '), CHR(9), ' '),
          AVG_EXE_USEC,
          EXECUTIONS,
          ELAPSED_TIME,
          BUFFERS_GETS,
          DISK_READS,
          HIT_COUNT,
          TYPE,
          TABLE_SCAN
        FROM GV$OB_PLAN_CACHE_PLAN_STAT
        WHERE QUERY_SQL IS NOT NULL
          AND LAST_ACTIVE_TIME >= TO_TIMESTAMP('{started_at}', 'YYYY-MM-DD HH24:MI:SS')
    """.format(started_at=started_at)
    ok, stdout, _ = _obclient_run_sql_on_source(
        config,
        query,
        timeout=config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT),
        session_query_timeout_us=config.settings.get(
            "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
        ),
    )
    if not ok or not stdout.strip():
        return []
    return _parse_source_plan_cache_recent_rows(stdout)


def build_source_plan_cache_recent_rows(recent_rows, captured_sql_ids, default_schema, captured_at):
    # type: (List[Dict[str, Any]], Iterable[str], str, str) -> List[Dict[str, Any]]
    captured_sql_id_set = set(str(sql_id) for sql_id in (captured_sql_ids or []) if sql_id)
    rows = []
    for recent_row in recent_rows:
        sql_id = str(recent_row.get("sql_id") or "").strip()
        if not sql_id or sql_id in captured_sql_id_set:
            continue
        avg_exe_usec = float(recent_row.get("avg_exe_usec") or 0.0)
        if avg_exe_usec <= 0.0:
            continue
        query_sql = recent_row.get("query_sql") or ""
        buffer_gets = float(recent_row.get("buffer_gets") or 0.0)
        disk_reads = float(recent_row.get("disk_reads") or 0.0)
        executions = max(1.0, float(recent_row.get("executions") or 1.0))
        rows.append(
            {
                "sql_id": sql_id,
                "sql_text": query_sql,
                "sql_text_normalized": normalize_sql_text(query_sql),
                "bind_vars": {},
                "schema": default_schema,
                "source": "ob_plan_cache_recent",
                "captured_at": captured_at,
                "baseline_source_mode": SOURCE_DB_MODE_OCEANBASE,
                "baseline_avg_elapsed_us": avg_exe_usec,
                "baseline_avg_logical_reads": buffer_gets / executions,
                "oracle_executions": 1,
                "oracle_avg_elapsed_us": avg_exe_usec,
                "oracle_avg_logical_reads": buffer_gets / executions,
                "oracle_avg_physical_reads": disk_reads / executions,
                "oracle_plan_hash": None,
                "oracle_plan_rows": [],
                "source_execution_count": 1,
                "source_total_elapsed_us": avg_exe_usec,
                "source_ob_request_id": None,
                "source_ob_trace_id": None,
                "source_ob_queue_time_us": 0.0,
                "source_ob_get_plan_time_us": 0.0,
                "source_ob_execute_time_us": avg_exe_usec,
                "source_ob_net_time_us": 0.0,
                "source_ob_net_wait_time_us": 0.0,
                "source_ob_plan_type_raw": recent_row.get("type"),
                "source_ob_plan_type": PLAN_TYPE_NAMES.get(
                    str(recent_row.get("type") or ""), str(recent_row.get("type") or "")
                ),
                "source_ob_is_hit_plan": "1" if float(recent_row.get("hit_count") or 0.0) > 0.0 else "0",
                "source_ob_is_executor_rpc": "0",
                "source_ob_retry_cnt": 0.0,
                "source_ob_memstore_read_rows": 0.0,
                "source_ob_ssstore_read_rows": 0.0,
                "source_ob_bloom_filter_filtered": 0.0,
                "source_ob_logical_reads": buffer_gets / executions,
                "source_ob_physical_reads": disk_reads / executions,
                "source_ob_table_scan": recent_row.get("table_scan"),
            }
        )
    return rows


def parse_explain_plan_text(plan_text):
    # type: (str) -> List[Dict[str, Any]]
    rows = []
    for line in (plan_text or "").splitlines():
        stripped = line.strip()
        if not stripped.startswith("|") or stripped.startswith("|ID|"):
            continue
        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        if len(cells) < 2:
            continue
        if not cells[0].isdigit():
            continue
        operator = cells[1]
        name = cells[2] if len(cells) > 2 else ""
        rows.append(
            {
                "id": int(cells[0]),
                "operator": operator,
                "name": name,
            }
        )
    return rows


def parse_plan_monitor_rows(stdout_text):
    # type: (str) -> List[Dict[str, Any]]
    rows = []
    for line in (stdout_text or "").splitlines():
        fields = line.split("\t")
        if len(fields) < 6:
            continue
        try:
            plan_line_id = int(fields[0])
        except (TypeError, ValueError):
            continue
        rows.append(
            {
                "plan_line_id": plan_line_id,
                "operator": fields[1],
                "output_rows": _safe_float(fields[2]),
                "workarea_mem": _safe_float(fields[3]),
                "workarea_max_mem": _safe_float(fields[4]),
                "workarea_tempseg": _safe_float(fields[5]),
            }
        )
    return rows


def _normalize_result_row(row):
    # type: (Any) -> str
    if isinstance(row, (list, tuple)):
        values = row
    else:
        values = [row]
    return "\x1f".join("" if value is None else str(value) for value in values)


def verify_result_sets(source_rows, target_rows, sample_limit):
    # type: (Iterable[Any], Iterable[Any], int) -> Dict[str, Any]
    source_list = list(source_rows)
    target_list = list(target_rows)
    sample_limit = max(1, int(sample_limit or 1))
    if len(source_list) > sample_limit or len(target_list) > sample_limit:
        return {
            "status": "skipped",
            "reason": "too_large",
            "source_hash": "",
            "target_hash": "",
            "mismatch_sample": [],
        }
    normalized_source = sorted(_normalize_result_row(row) for row in source_list)
    normalized_target = sorted(_normalize_result_row(row) for row in target_list)
    source_hash = hashlib.md5("\n".join(normalized_source).encode("utf-8")).hexdigest()
    target_hash = hashlib.md5("\n".join(normalized_target).encode("utf-8")).hexdigest()
    if source_hash == target_hash:
        return {
            "status": "match",
            "reason": "",
            "source_hash": source_hash,
            "target_hash": target_hash,
            "mismatch_sample": [],
        }
    mismatch_sample = []
    max_rows = max(len(normalized_source), len(normalized_target))
    for idx in range(max_rows):
        src = normalized_source[idx] if idx < len(normalized_source) else None
        tgt = normalized_target[idx] if idx < len(normalized_target) else None
        if src != tgt:
            mismatch_sample.append({"source": src, "target": tgt})
        if len(mismatch_sample) >= min(20, sample_limit):
            break
    return {
        "status": "mismatch",
        "reason": "",
        "source_hash": source_hash,
        "target_hash": target_hash,
        "mismatch_sample": mismatch_sample,
    }


def is_select_statement(sql_text):
    # type: (str) -> bool
    normalized = normalize_sql_text(sql_text)
    return normalized.startswith("SELECT ") or normalized.startswith("WITH ")


def _fetch_source_rows(config, sql_text):
    # type: (AppConfig, str) -> List[Any]
    if config.settings.get("source_db_mode") == SOURCE_DB_MODE_OCEANBASE:
        ok, stdout, stderr = _obclient_run_sql_on_source(
            config,
            sql_text,
            timeout=config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT),
            session_query_timeout_us=config.settings.get(
                "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
            ),
        )
        if not ok:
            raise ConfigError("OceanBase source verification query failed: %s" % (stderr or stdout))
        return [tuple(line.split("\t")) for line in (stdout or "").splitlines() if line.strip()]
    connection = _open_oracle_connection(config)
    try:
        cursor = connection.cursor()
        cursor.execute(sql_text)
        rows = cursor.fetchall()
        cursor.close()
        return rows
    finally:
        connection.close()


def _fetch_target_rows(config, sql_text):
    # type: (AppConfig, str) -> List[Any]
    ok, stdout, stderr = obclient_run_sql(
        config.oceanbase_target,
        sql_text,
        timeout=config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT),
        session_query_timeout_us=config.settings.get(
            "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
        ),
    )
    if not ok:
        raise ConfigError("OceanBase target verification query failed: %s" % (stderr or stdout))
    return [tuple(line.split("\t")) for line in (stdout or "").splitlines() if line.strip()]


def perform_result_verification(config, workload_row, rendered_sql):
    # type: (AppConfig, Dict[str, Any], str) -> Dict[str, Any]
    sample_limit = int(config.settings.get("result_sample_limit", 10000) or 10000)
    source_rows = _fetch_source_rows(config, rendered_sql)
    target_rows = _fetch_target_rows(config, rendered_sql)
    result = verify_result_sets(source_rows, target_rows, sample_limit)
    if result.get("status") == "mismatch":
        mismatch_path = build_artifact_path(
            "mismatch",
            generate_run_id(),
            root_dir=config.settings["workloads_dir"],
        )
        append_jsonl(
            mismatch_path,
            {
                "sql_id": workload_row.get("sql_id"),
                "sql_text": workload_row.get("sql_text"),
                "verification": result,
                "captured_at": utc_now_iso(),
            },
        )
        result["artifact_path"] = str(mismatch_path)
    return result


def collect_plan_monitor_rows(config, audit_row, rendered_sql):
    # type: (AppConfig, Dict[str, Any], str) -> List[Dict[str, Any]]
    trace_id = str(audit_row.get("trace_id") or "")
    sql_text_snippet = normalize_sql_text(rendered_sql)[:80].replace("'", "''")
    filters = []
    if trace_id:
        filters.append("TRACE_ID = '%s'" % trace_id.replace("'", "''"))
    if sql_text_snippet:
        filters.append(
            "UPPER(REPLACE(REPLACE(REPLACE(STATEMENT, CHR(10), ' '), CHR(13), ' '), CHR(9), ' ')) LIKE '%%%s%%'"
            % sql_text_snippet
        )
    where_clause = " OR ".join(filters) if filters else "1 = 0"
    query = """
        SELECT * FROM (
          SELECT
            PLAN_LINE_ID,
            OPERATOR,
            OUTPUT_ROWS,
            WORKAREA_MEM,
            WORKAREA_MAX_MEM,
            WORKAREA_TEMPSEG
          FROM GV$SQL_PLAN_MONITOR
          WHERE {where_clause}
          ORDER BY PLAN_LINE_ID
        )
    """.format(where_clause=where_clause)
    ok, stdout, _ = obclient_run_sql(
        config.oceanbase_target,
        query,
        timeout=config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT),
        session_query_timeout_us=config.settings.get(
            "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
        ),
    )
    if not ok:
        return []
    return parse_plan_monitor_rows(stdout)


def collect_source_plan_monitor_rows(config, row):
    # type: (AppConfig, Dict[str, Any]) -> List[Dict[str, Any]]
    trace_id = str(row.get("source_ob_trace_id") or row.get("trace_id") or "")
    sql_text_snippet = normalize_sql_text(row.get("sql_text") or "")[:80].replace("'", "''")
    filters = []
    if trace_id:
        filters.append("TRACE_ID = '%s'" % trace_id.replace("'", "''"))
    if sql_text_snippet:
        filters.append(
            "UPPER(REPLACE(REPLACE(REPLACE(STATEMENT, CHR(10), ' '), CHR(13), ' '), CHR(9), ' ')) LIKE '%%%s%%'"
            % sql_text_snippet
        )
    if not filters:
        return []
    query = """
        SELECT * FROM (
          SELECT
            PLAN_LINE_ID,
            OPERATOR,
            OUTPUT_ROWS,
            WORKAREA_MEM,
            WORKAREA_MAX_MEM,
            WORKAREA_TEMPSEG
          FROM GV$SQL_PLAN_MONITOR
          WHERE {where_clause}
          ORDER BY PLAN_LINE_ID
        )
    """.format(where_clause=" OR ".join(filters))
    ok, stdout, _ = _obclient_run_sql_on_source(
        config,
        query,
        timeout=config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT),
        session_query_timeout_us=config.settings.get(
            "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
        ),
    )
    if not ok:
        return []
    return parse_plan_monitor_rows(stdout)


def should_collect_plan_monitor(row, slowdown_threshold):
    # type: (Dict[str, Any], float) -> bool
    if row.get("ob_status") != "ok":
        return False
    plan_type = str(row.get("ob_plan_type_raw") or "").strip().upper()
    if plan_type == "3" or str(row.get("ob_plan_type") or "").strip().upper() == "DISTRIBUTED":
        return True
    speedup_ratio = row.get("speedup_ratio")
    if speedup_ratio is not None and float(speedup_ratio) < float(slowdown_threshold):
        return True
    net_ratio = row.get("net_ratio")
    if net_ratio is not None and float(net_ratio) > 0.6:
        return True
    return False


def summarize_verification_evidence(row):
    # type: (Dict[str, Any]) -> str
    status = str(row.get("verification_status") or "n/a")
    if status == "mismatch" and row.get("verification_artifact_path"):
        return "%s:%s" % (status, row.get("verification_artifact_path"))
    if status == "skipped" and row.get("verification_reason"):
        return "%s:%s" % (status, row.get("verification_reason"))
    return status


def summarize_plan_monitor_evidence(row):
    # type: (Dict[str, Any]) -> str
    monitor_rows = row.get("plan_monitor_rows") or []
    if not monitor_rows:
        return "n/a"
    operators = [
        str(monitor_row.get("operator") or "").strip()
        for monitor_row in monitor_rows[:3]
        if str(monitor_row.get("operator") or "").strip()
    ]
    summary = ",".join(operators) or "n/a"
    if any(float(monitor_row.get("workarea_tempseg") or 0.0) > 0.0 for monitor_row in monitor_rows):
        summary = "%s|spill" % summary if summary != "n/a" else "spill"
    return summary


def is_plsql_statement(sql_text):
    # type: (str) -> bool
    normalized = normalize_sql_text(sql_text)
    if normalized.startswith("BEGIN ") or normalized.startswith("DECLARE "):
        return True
    if normalized.startswith("CALL ") or normalized.startswith("EXEC ") or normalized.startswith("EXECUTE "):
        return True
    return False


def _normalize_oracle_plan_operator(plan_row):
    # type: (Dict[str, Any]) -> str
    parts = [str(plan_row.get("operation") or "").strip(), str(plan_row.get("options") or "").strip()]
    return " ".join(part for part in parts if part).strip().upper()


def _normalize_ob_plan_operator(plan_row):
    # type: (Dict[str, Any]) -> str
    return str(plan_row.get("operator") or "").strip().upper()


def build_plan_diff_signals(row):
    # type: (Dict[str, Any]) -> List[Dict[str, str]]
    oracle_ops = [_normalize_oracle_plan_operator(plan_row) for plan_row in (row.get("oracle_plan_rows") or [])]
    ob_ops = [_normalize_ob_plan_operator(plan_row) for plan_row in (row.get("ob_plan_rows") or [])]
    signals = []
    ob_plan_type = str(row.get("ob_plan_type_raw") or row.get("ob_plan_type") or "").strip().upper()
    if (
        any("TABLE ACCESS BY INDEX ROWID" in op for op in oracle_ops)
        and any("TABLE LOOKUP" in op for op in ob_ops)
    ):
        signals.append(
            {
                "signal_id": "LOOKUP-RISK",
                "severity": "high",
                "message": "Oracle indexed row access became OceanBase TABLE LOOKUP, which can add cross-node lookup cost.",
            }
        )
    if (
        any("NESTED LOOPS" in op for op in oracle_ops)
        and any("JOIN (NL)" in op or "NESTED LOOP" in op for op in ob_ops)
        and (ob_plan_type == "3" or ob_plan_type == "DISTRIBUTED")
    ):
        signals.append(
            {
                "signal_id": "NL-DIST-RISK",
                "severity": "high",
                "message": "Nested-loop style access is running under a distributed OceanBase plan and may amplify RPC cost.",
            }
        )
    if (
        any("PARTITION RANGE SINGLE" in op for op in oracle_ops)
        and not any("PARTITION SCAN" in op for op in ob_ops)
    ):
        signals.append(
            {
                "signal_id": "PARTITION-PRUNE-MISS",
                "severity": "medium",
                "message": "Oracle single-partition access did not map cleanly to OceanBase partition scan semantics.",
            }
        )
    monitor_rows = row.get("plan_monitor_rows") or []
    if monitor_rows:
        output_values = [float(monitor_row.get("output_rows") or 0.0) for monitor_row in monitor_rows]
        positive_values = [value for value in output_values if value > 0.0]
        if len(positive_values) > 1:
            avg_output = sum(positive_values) / float(len(positive_values))
            if avg_output > 0.0 and max(positive_values) > avg_output * 2.0:
                signals.append(
                    {
                        "signal_id": "PX-SKEW",
                        "severity": "high",
                        "message": "Plan monitor shows operator output skew above 200% of the average worker output.",
                    }
                )
        if any(float(monitor_row.get("workarea_tempseg") or 0.0) > 0.0 for monitor_row in monitor_rows):
            signals.append(
                {
                    "signal_id": "SPILL-RISK",
                    "severity": "high",
                    "message": "Plan monitor shows workarea temp spill for at least one operator.",
                }
            )
    return signals


def summarize_plan_diff_signals(row):
    # type: (Dict[str, Any]) -> str
    signals = row.get("plan_diff_signals")
    if signals is None:
        signals = build_plan_diff_signals(row)
    if not signals:
        return "n/a"
    return ",".join(str(signal.get("signal_id") or "") for signal in signals if signal.get("signal_id")) or "n/a"


def summarize_plsql_profile(row):
    # type: (Dict[str, Any]) -> str
    if row.get("plsql_profile_summary"):
        return str(row.get("plsql_profile_summary"))
    if row.get("plsql_profile_diagnosis_summary"):
        return str(row.get("plsql_profile_diagnosis_summary"))
    if row.get("plsql_profile_status") != "ok":
        if row.get("plsql_profile_status"):
            return str(row.get("plsql_profile_status"))
        return "n/a"
    top_lines = row.get("plsql_profile_top_lines") or []
    if not top_lines:
        return "ok"
    top_line = top_lines[0]
    for candidate in top_lines:
        source_text = str(candidate.get("source_text") or "").strip()
        if source_text and source_text.upper() != "NULL":
            top_line = candidate
            break
    return "%s:%s:%s" % (
        top_line.get("unit_name") or "unit",
        top_line.get("line") or "?",
        str(top_line.get("source_text") or "").strip()[:80],
    )


def summarize_plsql_profile_mapping(row):
    # type: (Dict[str, Any]) -> str
    if row.get("plsql_profile_mapping_summary"):
        return str(row.get("plsql_profile_mapping_summary"))
    if row.get("plsql_profile_status") != "ok":
        return "n/a"
    top_lines = row.get("plsql_profile_top_lines") or []
    if not top_lines:
        return "n/a"
    top_line = top_lines[0]
    confidence = str(top_line.get("source_mapping_confidence") or "").strip()
    strategy = str(top_line.get("source_mapping_strategy") or "").strip()
    if confidence and strategy:
        return "%s@%s" % (confidence, strategy)
    return confidence or strategy or "n/a"


def summarize_plsql_profile_diagnosis(row):
    # type: (Dict[str, Any]) -> str
    summary = str(row.get("plsql_profile_diagnosis_summary") or "").strip()
    return summary or "n/a"


def _format_profiler_line_range(start_line, end_line):
    # type: (Any, Any) -> str
    start_value = _safe_int(start_line)
    end_value = _safe_int(end_line)
    if start_value is None and end_value is None:
        return "?"
    if end_value is None or start_value == end_value:
        return str(start_value if start_value is not None else end_value)
    return "%s-%s" % (start_value, end_value)


def _collect_profiler_source_map(lines):
    # type: (List[Dict[str, Any]]) -> Dict[int, str]
    source_map = {}
    for line in lines:
        line_no = _safe_int(line.get("line"))
        source_text = _normalize_source_line_text(line.get("source_text"))
        if line_no is not None and source_text and line_no not in source_map:
            source_map[line_no] = source_text
        for context_row in line.get("context_lines") or []:
            context_line = _safe_int(context_row.get("line"))
            if context_line is None:
                continue
            context_text = _normalize_source_line_text(context_row.get("text"))
            if context_line not in source_map or not source_map.get(context_line):
                source_map[context_line] = context_text
    return source_map


def _build_profiler_block_excerpt(source_map, start_line, end_line):
    # type: (Dict[int, str], int, int) -> str
    parts = []
    for line_no in range(int(start_line), int(end_line) + 1):
        text = str(source_map.get(line_no) or "").strip()
        if text:
            parts.append(text)
    return " | ".join(parts[:4])[:220]


def _contains_profiler_pattern(source_text, patterns):
    # type: (str, Sequence[str]) -> bool
    upper_text = str(source_text or "").upper()
    for pattern in patterns:
        if re.search(pattern, upper_text):
            return True
    return False


def _diagnose_profiler_block(block):
    # type: (Dict[str, Any]) -> List[Dict[str, Any]]
    source_map = block.get("source_map") or {}
    ordered_lines = [source_map[line_no] for line_no in sorted(source_map.keys())]
    source_text = "\n".join(str(line or "") for line in ordered_lines)
    has_loop = _contains_profiler_pattern(source_text, [r"\bFOR\b", r"\bWHILE\b", r"\bLOOP\b"])
    has_dynamic_sql = _contains_profiler_pattern(source_text, [r"EXECUTE\s+IMMEDIATE", r"DBMS_SQL"])
    has_dml = _contains_profiler_pattern(
        source_text,
        [r"\bINSERT\b", r"\bUPDATE\b", r"\bDELETE\b", r"\bMERGE\b", r"\bSELECT\b.+\bINTO\b"],
    )
    has_commit = _contains_profiler_pattern(source_text, [r"\bCOMMIT\b", r"\bROLLBACK\b"])
    has_cpu_expression = _contains_profiler_pattern(
        source_text,
        [r"\bSQRT\b", r"\bSUBSTR\b", r"\bREGEXP_", r"\bTO_CHAR\b", r"\bTO_NUMBER\b", r"\bDBMS_LOB\b"],
    )
    diagnoses = []
    line_range = _format_profiler_line_range(block.get("start_line"), block.get("end_line"))
    base_payload = {
        "owner": block.get("owner"),
        "unit_name": block.get("unit_name"),
        "unit_type": block.get("unit_type"),
        "line_range": line_range,
        "total_time_us": block.get("total_time_us"),
        "total_occur": block.get("total_occur"),
        "source_excerpt": block.get("source_excerpt"),
    }

    if has_loop and has_dynamic_sql:
        diagnoses.append(
            dict(
                base_payload,
                diagnosis_id="dynamic_sql_in_loop",
                severity="high",
                message="Dynamic SQL is executed inside a hot loop.",
            )
        )
    if has_loop and has_commit and float(block.get("total_occur") or 0.0) >= 100.0:
        diagnoses.append(
            dict(
                base_payload,
                diagnosis_id="frequent_commit_in_loop",
                severity="high",
                message="Commit appears inside a high-frequency loop.",
            )
        )
    if has_loop and (has_dml or has_dynamic_sql):
        diagnoses.append(
            dict(
                base_payload,
                diagnosis_id="row_by_row_sql_in_loop",
                severity="high",
                message="Loop body performs SQL row by row instead of batching work.",
            )
        )
        diagnoses.append(
            dict(
                base_payload,
                diagnosis_id="bulk_candidate",
                severity="medium",
                message="Hot loop is a candidate for BULK COLLECT, FORALL, or MERGE rewrite.",
            )
        )
    if has_loop and has_cpu_expression and not (has_dml or has_dynamic_sql or has_commit) and float(block.get("total_occur") or 0.0) >= 1000.0:
        diagnoses.append(
            dict(
                base_payload,
                diagnosis_id="tight_cpu_loop",
                severity="medium",
                message="A tight CPU loop dominates profiler time without SQL offload.",
            )
        )
    return diagnoses


def _is_significant_profiler_block(block):
    # type: (Dict[str, Any]) -> bool
    return float(block.get("profile_time_ratio") or 0.0) >= 0.001


def analyze_plsql_profile_evidence(top_lines, unit_summary):
    # type: (List[Dict[str, Any]], List[Dict[str, Any]]) -> Dict[str, Any]
    normalized_units = [dict(item) for item in (unit_summary or [])]
    total_profile_time_us = sum(float(item.get("total_time_us") or 0.0) for item in normalized_units)
    if total_profile_time_us <= 0.0:
        total_profile_time_us = sum(float(item.get("total_time_us") or 0.0) for item in (top_lines or []))
    if total_profile_time_us <= 0.0:
        total_profile_time_us = 1.0
    for item in normalized_units:
        if item.get("profile_time_ratio") in (None, ""):
            item["profile_time_ratio"] = float(item.get("total_time_us") or 0.0) / total_profile_time_us
    by_unit = {}
    for line in top_lines or []:
        line_no = _safe_int(line.get("line"))
        if line_no is None:
            continue
        key = (line.get("owner"), line.get("unit_name"), line.get("unit_type"))
        by_unit.setdefault(key, []).append(dict(line))

    hot_blocks = []
    for (owner, unit_name, unit_type), lines in by_unit.items():
        sorted_lines = sorted(lines, key=lambda item: int(item.get("line") or 0))
        current_block = []
        current_last_line = None
        for line in sorted_lines:
            line_no = int(line.get("line") or 0)
            if current_block and current_last_line is not None and line_no > current_last_line + 2:
                hot_blocks.append(
                    {
                        "owner": owner,
                        "unit_name": unit_name,
                        "unit_type": unit_type,
                        "lines": list(current_block),
                    }
                )
                current_block = []
            current_block.append(line)
            current_last_line = line_no
        if current_block:
            hot_blocks.append(
                {
                    "owner": owner,
                    "unit_name": unit_name,
                    "unit_type": unit_type,
                    "lines": list(current_block),
                }
            )

    for block in hot_blocks:
        hot_lines = block.get("lines") or []
        block_line_numbers = [int(item.get("line") or 0) for item in hot_lines if _safe_int(item.get("line")) is not None]
        block["start_line"] = min(block_line_numbers) if block_line_numbers else None
        block["end_line"] = max(block_line_numbers) if block_line_numbers else None
        block["total_time_us"] = sum(float(item.get("total_time_us") or 0.0) for item in hot_lines)
        block["total_occur"] = sum(float(item.get("total_occur") or 0.0) for item in hot_lines)
        block["profile_time_ratio"] = float(block.get("total_time_us") or 0.0) / total_profile_time_us
        block["source_map"] = _collect_profiler_source_map(hot_lines)
        block["source_excerpt"] = _build_profiler_block_excerpt(
            block["source_map"],
            block.get("start_line") or 0,
            block.get("end_line") or 0,
        )
        diagnoses = _diagnose_profiler_block(block) if _is_significant_profiler_block(block) else []
        block["diagnosis_ids"] = [item["diagnosis_id"] for item in diagnoses]
        block["primary_diagnosis_id"] = diagnoses[0]["diagnosis_id"] if diagnoses else ""
        block["diagnosis_summary"] = (
            "%s:%s:%s"
            % (
                block.get("unit_name") or "unit",
                _format_profiler_line_range(block.get("start_line"), block.get("end_line")),
                diagnoses[0]["diagnosis_id"],
            )
            if diagnoses
            else ""
        )
        block["lines"] = hot_lines

    hot_blocks = sorted(
        hot_blocks,
        key=lambda item: (
            -float(item.get("total_time_us") or 0.0),
            str(item.get("unit_name") or ""),
            int(item.get("start_line") or 0),
        ),
    )
    diagnoses = []
    for block in hot_blocks:
        block_diagnoses = _diagnose_profiler_block(block) if _is_significant_profiler_block(block) else []
        diagnoses.extend(block_diagnoses)
    diagnoses = sorted(
        diagnoses,
        key=lambda item: (
            {"high": 0, "medium": 1, "low": 2}.get(str(item.get("severity") or "low"), 3),
            -float(item.get("total_time_us") or 0.0),
        ),
    )
    diagnosis_summary = " | ".join(
        "%s:%s:%s" % (item.get("unit_name") or "unit", item.get("line_range") or "?", item.get("diagnosis_id") or "")
        for item in diagnoses[:3]
    )
    return {
        "unit_summary": normalized_units,
        "hot_blocks": hot_blocks,
        "diagnoses": diagnoses,
        "diagnosis_summary": diagnosis_summary,
    }


def _sanitize_filename_fragment(value):
    # type: (Any) -> str
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "").strip()).strip("._")
    return cleaned or "unknown"


def _format_external_template(template, values):
    # type: (str, Dict[str, Any]) -> str
    def replace(match):
        # type: (Any) -> str
        return str(values.get(match.group(1)) or "")

    return re.sub(r"\{([A-Za-z0-9_]+)\}", replace, str(template or ""))


def _build_external_diagnostic_path(report_dir, run_id, sql_id, provider, suffix):
    # type: (Union[str, Path], str, str, str, str) -> Path
    return Path(report_dir) / (
        "external_diag_%s_%s_%s%s"
        % (
            _sanitize_filename_fragment(run_id),
            _sanitize_filename_fragment(sql_id),
            _sanitize_filename_fragment(provider),
            suffix,
        )
    )


def _summarize_external_payload(payload_text):
    # type: (str) -> str
    raw = str(payload_text or "").strip()
    if not raw:
        return "empty"
    try:
        payload = json.loads(raw)
    except Exception:
        return build_sql_preview(raw, limit=80)
    if isinstance(payload, dict):
        interesting_keys = [key for key in ("status", "message", "sql_id", "trace_id", "tenant") if key in payload]
        if interesting_keys:
            return ",".join("%s=%s" % (key, payload.get(key)) for key in interesting_keys)
        return "json:%s_keys" % len(payload.keys())
    if isinstance(payload, list):
        return "json:list[%s]" % len(payload)
    return build_sql_preview(raw, limit=80)


def _summarize_external_entry(entry):
    # type: (Optional[Dict[str, Any]]) -> str
    if not entry:
        return "n/a"
    status = str(entry.get("status") or "n/a")
    summary = str(entry.get("summary") or "").strip()
    if summary:
        return "%s:%s" % (status, summary)
    return status


def summarize_external_diagnostics(row):
    # type: (Dict[str, Any]) -> str
    ocp_summary = _summarize_external_entry(row.get("ocp_diagnostic"))
    obdiag_summary = _summarize_external_entry(row.get("obdiag_diagnostic"))
    if ocp_summary in ("n/a", "unconfigured") and obdiag_summary in ("n/a", "unconfigured"):
        return "n/a"
    return "ocp=%s | obdiag=%s" % (ocp_summary, obdiag_summary)


def _merge_external_diagnostics(row, diagnostics):
    # type: (Dict[str, Any], Dict[str, Any]) -> Dict[str, Any]
    merged = dict(row)
    merged["ocp_diagnostic"] = diagnostics.get("ocp") or merged.get("ocp_diagnostic") or {}
    merged["obdiag_diagnostic"] = diagnostics.get("obdiag") or merged.get("obdiag_diagnostic") or {}
    return merged


def _should_collect_external_row_diagnostics(row, slowdown_threshold):
    # type: (Dict[str, Any], float) -> bool
    if str(row.get("ob_status") or "ok") != "ok":
        return False
    if should_collect_plan_monitor(row, slowdown_threshold):
        return True
    source_total_elapsed_us = _safe_float(row.get("source_total_elapsed_us"))
    if source_total_elapsed_us is not None and source_total_elapsed_us > 0.0:
        return True
    return False


def _build_external_context(row, run_id):
    # type: (Dict[str, Any], str) -> Dict[str, Any]
    return {
        "run_id": run_id,
        "sql_id": str(row.get("sql_id") or ""),
        "trace_id": str(row.get("source_ob_trace_id") or row.get("trace_id") or ""),
        "request_id": str(row.get("source_ob_request_id") or row.get("request_id") or ""),
        "schema": str(row.get("schema") or ""),
    }


def _fetch_ocp_cluster_inventory(config):
    # type: (AppConfig) -> List[Dict[str, Any]]
    base_url = str(config.settings.get("ocp_base_url") or "").rstrip("/")
    if not base_url:
        return []
    request = urllib.request.Request(
        "%s/api/v2/ob/clusters" % base_url,
        headers=_build_ocp_headers(config),
    )
    response = _open_ocp_request(
        config,
        request,
        int(config.settings.get("ocp_timeout", DEFAULT_OCP_TIMEOUT)),
    )
    body = response.read().decode("utf-8", errors="ignore")
    try:
        payload = json.loads(body)
    except Exception:
        return []
    return _extract_ocp_contents(payload)


def resolve_ocp_target_ids(config):
    # type: (AppConfig) -> Dict[str, str]
    cluster_id = str(config.settings.get("ocp_cluster_id") or "").strip()
    tenant_id = str(config.settings.get("ocp_tenant_id") or "").strip()
    if cluster_id and tenant_id:
        return {"cluster_id": cluster_id, "tenant_id": tenant_id}
    cached = config.settings.get("_resolved_ocp_target_ids")
    if isinstance(cached, dict) and cached.get("cluster_id") and cached.get("tenant_id"):
        return {
            "cluster_id": str(cached.get("cluster_id")),
            "tenant_id": str(cached.get("tenant_id")),
        }
    cluster_name = str(config.settings.get("ocp_cluster_name") or "").strip()
    tenant_name = str(config.settings.get("ocp_tenant_name") or "").strip()
    if not cluster_name or not tenant_name:
        return {"cluster_id": cluster_id, "tenant_id": tenant_id}
    inventory = _fetch_ocp_cluster_inventory(config)
    for cluster in inventory:
        if str(cluster.get("name") or "").strip() != cluster_name:
            continue
        resolved_cluster_id = str(cluster.get("id") or "").strip()
        for tenant in cluster.get("tenants") or []:
            if str(tenant.get("name") or "").strip() == tenant_name:
                resolved = {
                    "cluster_id": resolved_cluster_id,
                    "tenant_id": str(tenant.get("id") or "").strip(),
                }
                config.settings["_resolved_ocp_target_ids"] = resolved
                return resolved
    return {"cluster_id": cluster_id, "tenant_id": tenant_id}


def _build_ocp_headers(config):
    # type: (AppConfig) -> Dict[str, str]
    headers = {}
    authorization_env = str(config.settings.get("ocp_authorization_env") or "").strip()
    if authorization_env and os.environ.get(authorization_env):
        headers["Authorization"] = os.environ.get(authorization_env) or ""
        return headers
    username = str(config.settings.get("ocp_username") or "").strip()
    password_env = str(config.settings.get("ocp_password_env") or "").strip()
    password = ""
    if password_env and os.environ.get(password_env):
        password = os.environ.get(password_env) or ""
    elif config.settings.get("ocp_password"):
        password = str(config.settings.get("ocp_password") or "")
    if username and password:
        encoded = base64.b64encode(("%s:%s" % (username, password)).encode("utf-8")).decode("ascii")
        headers["Authorization"] = "Basic %s" % encoded
        return headers
    token_env = str(config.settings.get("ocp_auth_token_env") or "").strip()
    token_value = os.environ.get(token_env) if token_env else ""
    if token_value:
        headers["Authorization"] = "Bearer %s" % token_value
    return headers


def _open_ocp_request(config, request, timeout_seconds):
    # type: (AppConfig, Any, int) -> Any
    if config.settings.get("ocp_verify_tls", True):
        return urllib.request.urlopen(request, timeout=timeout_seconds)
    context = ssl._create_unverified_context()  # type: ignore[attr-defined]
    return urllib.request.urlopen(request, timeout=timeout_seconds, context=context)


def _parse_iso_datetime(value):
    # type: (Any) -> Optional[datetime]
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _extract_ocp_contents(payload):
    # type: (Any) -> List[Dict[str, Any]]
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict):
            contents = data.get("contents")
            if isinstance(contents, list):
                return [item for item in contents if isinstance(item, dict)]
        if isinstance(payload.get("contents"), list):
            return [item for item in payload.get("contents") if isinstance(item, dict)]
    return []


def _first_non_empty(mapping, keys):
    # type: (Dict[str, Any], Sequence[str]) -> Any
    for key in keys:
        value = mapping.get(key)
        if value not in (None, ""):
            return value
    return None


def _build_ocp_native_base(config):
    # type: (AppConfig) -> str
    return str(config.settings.get("ocp_base_url") or "").rstrip("/")


def _build_ocp_native_sql_list_url(config, path_name, row):
    # type: (AppConfig, str, Dict[str, Any]) -> str
    resolved = resolve_ocp_target_ids(config)
    replayed_at = _parse_iso_datetime(row.get("replayed_at") or row.get("captured_at")) or datetime.now(timezone.utc)
    if replayed_at.tzinfo is None:
        replayed_at = replayed_at.replace(tzinfo=timezone.utc)
    window_minutes = max(1, min(1440, int(config.settings.get("ocp_window_minutes", 15))))
    start_time = replayed_at - timedelta(minutes=window_minutes)
    end_time = replayed_at + timedelta(minutes=window_minutes)
    query = {
        "startTime": start_time.isoformat(),
        "endTime": end_time.isoformat(),
        "sqlText": build_sql_preview(row.get("sql_text"), limit=120),
        "page": 1,
        "size": max(1, int(config.settings.get("ocp_query_limit", 20))),
    }
    return "%s/api/v2/ob/clusters/%s/tenants/%s/%s?%s" % (
        _build_ocp_native_base(config),
        resolved.get("cluster_id"),
        resolved.get("tenant_id"),
        path_name,
        urllib.parse.urlencode(query),
    )


def _build_ocp_native_sql_text_url(config, ocp_sql_id):
    # type: (AppConfig, str) -> str
    resolved = resolve_ocp_target_ids(config)
    return "%s/api/v2/ob/clusters/%s/tenants/%s/sql/%s/text" % (
        _build_ocp_native_base(config),
        resolved.get("cluster_id"),
        resolved.get("tenant_id"),
        urllib.parse.quote(str(ocp_sql_id or ""), safe=""),
    )


def _collect_ocp_native_row_diagnostics(config, row, run_id):
    # type: (AppConfig, Dict[str, Any], str) -> Dict[str, Any]
    resolved_ids = resolve_ocp_target_ids(config)
    if not resolved_ids.get("cluster_id") or not resolved_ids.get("tenant_id"):
        return {"status": "misconfigured", "summary": "native OCP cluster/tenant could not be resolved", "artifact_path": ""}
    context = _build_external_context(row, run_id)
    report_dir = config.settings["report_dir"]
    timeout_seconds = int(config.settings.get("ocp_timeout", DEFAULT_OCP_TIMEOUT))
    headers = _build_ocp_headers(config)
    payload_manifest = {}  # type: Dict[str, Any]
    candidate = None  # type: Optional[Dict[str, Any]]
    candidate_source = ""
    summaries = []
    for provider_name, path_name in (("top_sql", "topSql"), ("slow_sql", "slowSql")):
        url = _build_ocp_native_sql_list_url(config, path_name, row)
        request = urllib.request.Request(url, headers=headers)
        try:
            response = _open_ocp_request(config, request, timeout_seconds)
            body = response.read().decode("utf-8", errors="ignore")
        except Exception as exc:
            return {"status": "error", "summary": str(exc), "artifact_path": ""}
        artifact_path = _build_external_diagnostic_path(report_dir, run_id, context["sql_id"], provider_name, ".json")
        write_text(artifact_path, body)
        payload = {}
        try:
            payload = json.loads(body)
        except Exception:
            payload = {}
        contents = _extract_ocp_contents(payload)
        payload_manifest[provider_name] = {
            "url": url,
            "artifact_path": str(artifact_path),
            "match_count": len(contents),
        }
        summaries.append("%s=%s" % (provider_name, len(contents)))
        if contents and candidate is None:
            candidate = contents[0]
            candidate_source = provider_name
    if candidate is None:
        manifest_path = _build_external_diagnostic_path(report_dir, run_id, context["sql_id"], "ocp_native", ".json")
        write_json(
            manifest_path,
            {
                "provider": "native_ocp",
                "resolved_target": resolved_ids,
                "payloads": payload_manifest,
            },
        )
        return {
            "status": "no-match",
            "summary": "cluster=%s tenant=%s %s"
            % (resolved_ids.get("cluster_id"), resolved_ids.get("tenant_id"), "; ".join(summaries)),
            "artifact_path": str(manifest_path),
        }
    ocp_sql_id = _first_non_empty(candidate, ("sqlId", "sql_id", "id"))
    if ocp_sql_id:
        text_url = _build_ocp_native_sql_text_url(config, str(ocp_sql_id))
        request = urllib.request.Request(text_url, headers=headers)
        try:
            response = _open_ocp_request(config, request, timeout_seconds)
            body = response.read().decode("utf-8", errors="ignore")
        except Exception as exc:
            return {"status": "error", "summary": str(exc), "artifact_path": ""}
        text_artifact_path = _build_external_diagnostic_path(report_dir, run_id, context["sql_id"], "sql_text", ".json")
        write_text(text_artifact_path, body)
        payload_manifest["sql_text"] = {"url": text_url, "artifact_path": str(text_artifact_path)}
        trends_url = "%s/api/v2/ob/clusters/%s/tenants/%s/sqls/%s/trends?startTime=%s&endTime=%s"
        replayed_at = _parse_iso_datetime(row.get("replayed_at") or row.get("captured_at")) or datetime.now(timezone.utc)
        if replayed_at.tzinfo is None:
            replayed_at = replayed_at.replace(tzinfo=timezone.utc)
        window_minutes = max(1, min(1440, int(config.settings.get("ocp_window_minutes", 15))))
        trend_start = (replayed_at - timedelta(minutes=window_minutes)).isoformat()
        trend_end = replayed_at.isoformat()
        trends_url = trends_url % (
            _build_ocp_native_base(config),
            resolved_ids.get("cluster_id"),
            resolved_ids.get("tenant_id"),
            urllib.parse.quote(str(ocp_sql_id or ""), safe=""),
            urllib.parse.quote(trend_start, safe=":"),
            urllib.parse.quote(trend_end, safe=":"),
        )
        trends_request = urllib.request.Request(trends_url, headers=headers)
        try:
            trends_response = _open_ocp_request(config, trends_request, timeout_seconds)
            trends_body = trends_response.read().decode("utf-8", errors="ignore")
            trends_artifact_path = _build_external_diagnostic_path(report_dir, run_id, context["sql_id"], "sql_trends", ".json")
            write_text(trends_artifact_path, trends_body)
            payload_manifest["sql_trends"] = {"url": trends_url, "artifact_path": str(trends_artifact_path)}
        except Exception as exc:
            payload_manifest["sql_trends"] = {"url": trends_url, "error": str(exc)}
    manifest_path = _build_external_diagnostic_path(report_dir, run_id, context["sql_id"], "ocp_native", ".json")
    write_json(
        manifest_path,
        {
            "provider": "native_ocp",
            "resolved_target": resolved_ids,
            "selected_source": candidate_source,
            "selected_sql_id": ocp_sql_id,
            "selected_candidate": candidate,
            "payloads": payload_manifest,
        },
    )
    summary = "cluster=%s tenant=%s %s sqlId=%s hits=%s" % (
        resolved_ids.get("cluster_id"),
        resolved_ids.get("tenant_id"),
        candidate_source or "top_sql",
        ocp_sql_id or "n/a",
        "; ".join(summaries),
    )
    return {"status": "ok", "summary": summary, "artifact_path": str(manifest_path)}


def _collect_ocp_row_diagnostics(config, row, run_id):
    # type: (AppConfig, Dict[str, Any], str) -> Dict[str, Any]
    capability = _probe_ocp_capability(config)
    if capability.get("status") != "ready":
        return {"status": str(capability.get("status") or "unconfigured"), "summary": str(capability.get("error") or ""), "artifact_path": ""}
    if capability.get("mode") == "native":
        return _collect_ocp_native_row_diagnostics(config, row, run_id)
    context = _build_external_context(row, run_id)
    report_dir = config.settings["report_dir"]
    timeout_seconds = int(config.settings.get("ocp_timeout", DEFAULT_OCP_TIMEOUT))
    headers = _build_ocp_headers(config)
    payload = {}
    summaries = []
    for provider_name, template_key in (("ash", "ocp_ash_url_template"), ("qpm", "ocp_qpm_url_template")):
        template = str(config.settings.get(template_key) or "").strip()
        if not template:
            continue
        url = _format_external_template(template, context)
        request = urllib.request.Request(url, headers=headers)
        try:
            response = _open_ocp_request(config, request, timeout_seconds)
            body = response.read().decode("utf-8", errors="ignore")
        except Exception as exc:
            return {"status": "error", "summary": str(exc), "artifact_path": ""}
        artifact_path = _build_external_diagnostic_path(report_dir, run_id, context["sql_id"], provider_name, ".txt")
        write_text(artifact_path, body)
        summary = _summarize_external_payload(body)
        payload[provider_name] = {"url": url, "artifact_path": str(artifact_path), "summary": summary}
        summaries.append("%s:%s" % (provider_name, summary))
    if not payload:
        return {"status": "unconfigured", "summary": "", "artifact_path": ""}
    return {"status": "ok", "summary": "; ".join(summaries), "artifact_path": json.dumps(payload, sort_keys=True), "payload": payload}


def _collect_obdiag_row_diagnostics(config, row, run_id):
    # type: (AppConfig, Dict[str, Any], str) -> Dict[str, Any]
    capability = _probe_obdiag_capability(config)
    if capability.get("status") != "ready":
        return {"status": str(capability.get("status") or "unconfigured"), "summary": str(capability.get("error") or ""), "artifact_path": ""}
    context = _build_external_context(row, run_id)
    executable = str(config.settings.get("obdiag_executable") or "").strip()
    output_path = _build_external_diagnostic_path(config.settings["report_dir"], run_id, context["sql_id"], "obdiag", ".txt")
    args = [executable]
    extra_args = str(config.settings.get("obdiag_extra_args") or "").strip()
    if extra_args:
        for token in shlex.split(extra_args):
            args.append(_format_external_template(token, context))
    try:
        completed = subprocess.run(  # nosec - operator-provided binary path with explicit args
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=int(config.settings.get("obdiag_timeout", DEFAULT_OBDIAG_TIMEOUT)),
            check=False,
            universal_newlines=True,
        )
    except Exception as exc:
        return {"status": "error", "summary": str(exc), "artifact_path": ""}
    body = (completed.stdout or "") + ("\n" + completed.stderr if completed.stderr else "")
    write_text(output_path, body)
    status = "ok" if completed.returncode == 0 else "error"
    summary = _summarize_external_payload(body) if body.strip() else "exit=%s" % completed.returncode
    return {"status": status, "summary": summary, "artifact_path": str(output_path)}


def collect_external_row_diagnostics(config, row, run_id):
    # type: (AppConfig, Dict[str, Any], str) -> Dict[str, Any]
    return {
        "ocp": _collect_ocp_row_diagnostics(config, row, run_id),
        "obdiag": _collect_obdiag_row_diagnostics(config, row, run_id),
    }


def sql_literal(value):
    # type: (Any) -> str
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    return "'" + text.replace("'", "''") + "'"


def literalize_bind_vars(bind_vars):
    # type: (Optional[Dict[str, Any]]) -> Tuple[Dict[str, str], Optional[str]]
    rendered = {}
    if not bind_vars:
        return rendered, None
    unsupported_keys = []
    for key, value in bind_vars.items():
        if isinstance(value, (dict, list, set, tuple)):
            unsupported_keys.append(str(key))
            continue
        rendered[str(key)] = sql_literal(value)
    if unsupported_keys:
        return rendered, "unsupported bind types for keys: %s" % ", ".join(sorted(unsupported_keys))
    return rendered, None


def apply_bind_literals(sql_text, bind_vars):
    # type: (str, Optional[Dict[str, Any]]) -> str
    rendered = str(sql_text or "")
    literal_map, _ = literalize_bind_vars(bind_vars)
    if not literal_map:
        return rendered
    for key_text, literal in sorted(literal_map.items(), key=lambda item: len(str(item[0])), reverse=True):
        if key_text.isdigit():
            patterns = [
                r":B%s\b" % re.escape(key_text),
                r":%s\b" % re.escape(key_text),
            ]
        else:
            patterns = [r":%s\b" % re.escape(key_text)]
        for pattern in patterns:
            rendered = re.sub(pattern, literal, rendered, flags=re.IGNORECASE)
    return rendered


def render_sql_for_replay(sql_text, bind_vars):
    # type: (str, Optional[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]
    _, skip_reason = literalize_bind_vars(bind_vars)
    if skip_reason:
        return None, skip_reason
    rendered = apply_bind_literals(sql_text, bind_vars)
    normalized = str(rendered or "").strip()
    call_match = re.match(r"^CALL\s+(.+?)\s*;?\s*$", normalized, flags=re.I | re.S)
    if call_match:
        normalized = "BEGIN %s; END" % call_match.group(1).rstrip(";").strip()
    return normalized, None


def derive_replay_metrics(row):
    # type: (Dict[str, Any]) -> Dict[str, Any]
    oracle_elapsed = float(
        row.get("baseline_avg_elapsed_us")
        or row.get("oracle_avg_elapsed_us")
        or 0.0
    )
    ob_elapsed = float(row.get("ob_elapsed_us") or row.get("ob_wall_time_us") or 0.0)
    oracle_reads = float(
        row.get("baseline_avg_logical_reads")
        or row.get("oracle_avg_logical_reads")
        or 0.0
    )
    ob_reads = float(row.get("ob_logical_reads") or 0.0)
    ob_net = float(row.get("ob_net_time_us") or 0.0)
    ob_get_plan = float(row.get("ob_get_plan_time_us") or 0.0)
    queue_time = float(row.get("ob_queue_time_us") or 0.0)
    execute_time = float(row.get("ob_execute_time_us") or 0.0)
    memstore_rows = float(row.get("ob_memstore_read_rows") or 0.0)
    ssstore_rows = float(row.get("ob_ssstore_read_rows") or 0.0)
    speedup_ratio = None
    if oracle_elapsed > 0.0 and ob_elapsed > 0.0:
        speedup_ratio = oracle_elapsed / ob_elapsed
    read_amplification = None
    if oracle_reads > 0.0 and ob_reads >= 0.0:
        read_amplification = ob_reads / oracle_reads
    net_ratio = None
    if ob_elapsed > 0.0:
        net_ratio = ob_net / ob_elapsed
    plan_miss_ratio = None
    if ob_elapsed > 0.0:
        plan_miss_ratio = ob_get_plan / ob_elapsed
    queue_execute_ratio = None
    if execute_time > 0.0:
        queue_execute_ratio = queue_time / execute_time
    lsm_memstore_ratio = None
    if (memstore_rows + ssstore_rows) > 0.0:
        lsm_memstore_ratio = memstore_rows / (memstore_rows + ssstore_rows)
    return {
        "speedup_ratio": speedup_ratio,
        "read_amplification": read_amplification,
        "net_ratio": net_ratio,
        "plan_miss_ratio": plan_miss_ratio,
        "queue_execute_ratio": queue_execute_ratio,
        "lsm_memstore_ratio": lsm_memstore_ratio,
        "plan_changed": bool(
            row.get("oracle_plan_hash")
            and row.get("ob_plan_hash")
            and str(row.get("oracle_plan_hash")) != str(row.get("ob_plan_hash"))
        ),
        "ob_plan_type": PLAN_TYPE_NAMES.get(str(row.get("ob_plan_type_raw") or ""), str(row.get("ob_plan_type_raw") or "") or None),
    }


def _normalize_template_identifier(raw_text, fallback):
    # type: (str, str) -> str
    cleaned = re.sub(r"[^A-Za-z0-9_]+", "_", str(raw_text or "").strip().lower()).strip("_")
    if not cleaned:
        cleaned = str(fallback or "perf")
    return cleaned[:48]


def _is_simple_sql_identifier(value):
    # type: (Optional[str]) -> bool
    return bool(re.match(r"^[A-Za-z_][A-Za-z0-9_$#]*$", str(value or "").strip()))


def _format_qualified_object(schema_name, object_name):
    # type: (Optional[str], str) -> str
    if schema_name:
        return '"%s"."%s"' % (schema_name, object_name)
    return '"%s"' % object_name


def _format_template_object(schema_name, object_name, include_schema=True):
    # type: (Optional[str], str, bool) -> str
    normalized_object = str(object_name or "").strip() or "REPLACE_TABLE_NAME"
    normalized_schema = str(schema_name or "").strip()
    if include_schema and normalized_schema and _is_simple_sql_identifier(normalized_schema) and _is_simple_sql_identifier(normalized_object):
        return "%s.%s" % (normalized_schema, normalized_object)
    if not include_schema and _is_simple_sql_identifier(normalized_object):
        return normalized_object
    if include_schema and normalized_schema:
        return _format_qualified_object(normalized_schema, normalized_object)
    if _is_simple_sql_identifier(normalized_object):
        return normalized_object
    return '"%s"' % normalized_object


def _infer_recommendation_tables(row):
    # type: (Dict[str, Any]) -> List[Tuple[Optional[str], str]]
    tables = extract_table_references(row.get("sql_text") or "")
    if tables:
        return tables
    schema_name = str(row.get("schema") or "").strip() or None
    return [(schema_name, "REPLACE_TABLE_NAME")]


def _build_dist_join_hint_sql(row):
    # type: (Dict[str, Any]) -> str
    tablegroup_name = "tg_%s" % _normalize_template_identifier(
        row.get("sql_id") or "dist_join", "dist_join"
    )
    tables = _infer_recommendation_tables(row)[:2]
    lines = [
        "CREATE TABLEGROUP %s;" % tablegroup_name,
    ]
    for schema_name, object_name in tables:
        lines.append(
            "ALTER TABLE %s SET TABLEGROUP %s;"
            % (_format_template_object(schema_name, object_name, include_schema=False), tablegroup_name)
        )
    lines.append(
        "ALTER TABLEGROUP %s LOCALITY = 'F@zone1';" % tablegroup_name
    )
    return "\n".join(lines)


def _build_plan_miss_hint_sql(row):
    # type: (Dict[str, Any]) -> str
    tables = _infer_recommendation_tables(row)[:2]
    lines = []
    for schema_name, object_name in tables:
        owner = schema_name or str(row.get("schema") or "APP")
        lines.extend(
            [
                "BEGIN",
                "  DBMS_STATS.GATHER_TABLE_STATS(",
                "    ownname => '%s'," % owner,
                "    tabname => '%s'," % object_name,
                "    method_opt => 'FOR ALL COLUMNS SIZE AUTO'",
                "  );",
                "END;",
                "/",
                "",
            ]
        )
    lines.extend(
        [
            "CALL DBMS_XPLAN.ENABLE_OPT_TRACE();",
            "-- Execute the target SQL and capture the preferred outline or plan hash.",
            "CREATE OUTLINE outline_%s ON %s USING HINT /* REPLACE_WITH_CAPTURED_HINTS */;"
            % (
                _normalize_template_identifier(row.get("sql_id") or "plan_miss", "plan_miss"),
                _format_template_object(
                    tables[0][0] if tables else str(row.get("schema") or None),
                    tables[0][1] if tables else "REPLACE_TABLE_NAME",
                ),
            ),
        ]
    )
    return "\n".join(lines)


def _get_profiler_hotspot(row):
    # type: (Dict[str, Any]) -> Optional[Dict[str, Any]]
    if row.get("plsql_profile_status") != "ok":
        return None
    hot_lines = row.get("plsql_profile_top_lines") or []
    if not hot_lines:
        return None
    return hot_lines[0]


def _infer_profiler_target_table(hotspot):
    # type: (Optional[Dict[str, Any]]) -> str
    if not hotspot:
        return "REPLACE_TARGET_TABLE"
    source_candidates = []
    if hotspot.get("source_text"):
        source_candidates.append(hotspot.get("source_text"))
    for context_row in hotspot.get("context_lines") or []:
        if context_row.get("text"):
            source_candidates.append(context_row.get("text"))
    for candidate in source_candidates:
        tables = extract_table_references(candidate)
        if tables:
            _, object_name = tables[0]
            return object_name
    return "REPLACE_TARGET_TABLE"


def _build_plsql_rpc_hint_sql(row):
    # type: (Dict[str, Any]) -> str
    hotspot = _get_profiler_hotspot(row)
    unit_name = str(hotspot.get("unit_name") if hotspot else "REPLACE_PACKAGE")
    line_no = hotspot.get("line") if hotspot else "?"
    target_table = _infer_profiler_target_table(hotspot)
    context_text = build_sql_preview(
        hotspot.get("source_text") if hotspot else "row-by-row DML hotspot",
        limit=140,
    )
    return "\n".join(
        [
            "-- profiler_hotspot: %s line %s" % (unit_name, line_no),
            "-- source_line: %s" % context_text,
            "DECLARE",
            "  TYPE t_key_tab IS TABLE OF NUMBER INDEX BY PLS_INTEGER;",
            "  v_keys t_key_tab;",
            "BEGIN",
            "  -- Replace the cursor query below using the profiled package context.",
            "  SELECT <pk_col> BULK COLLECT INTO v_keys FROM %s WHERE <predicate>;" % target_table,
            "  FORALL i IN INDICES OF v_keys",
            "    UPDATE %s SET <target_col> = <new_value_expr> WHERE <pk_col> = v_keys(i);" % target_table,
            "END;",
            "/",
            "",
            "MERGE INTO %s t" % target_table,
            "USING (",
            "  SELECT <pk_col>, <new_value_expr> AS new_value",
            "  FROM <source_query>",
            ") s",
            "ON (t.<pk_col> = s.<pk_col>)",
            "WHEN MATCHED THEN UPDATE SET t.<target_col> = s.new_value;",
        ]
    )


def _build_plsql_dynamic_sql_hint_sql(row):
    # type: (Dict[str, Any]) -> str
    hotspot = _get_profiler_hotspot(row)
    unit_name = str(hotspot.get("unit_name") if hotspot else "REPLACE_PACKAGE")
    target_table = _infer_profiler_target_table(hotspot)
    return "\n".join(
        [
            "-- profiler_diagnosis: dynamic SQL executed inside a hot loop",
            "-- package: %s" % unit_name,
            "-- Prefer static SQL when object names are fixed; otherwise prepare the statement once per batch.",
            "DECLARE",
            "  v_stmt CONSTANT VARCHAR2(32767) := 'UPDATE %s SET <target_col> = :1 WHERE <pk_col> = :2';" % target_table,
            "BEGIN",
            "  FORALL i IN INDICES OF v_ids",
            "    EXECUTE IMMEDIATE v_stmt USING v_values(i), v_ids(i);",
            "END;",
            "/",
        ]
    )


def _build_plsql_commit_hot_hint_sql(row):
    # type: (Dict[str, Any]) -> str
    hotspot = _get_profiler_hotspot(row)
    unit_name = str(hotspot.get("unit_name") if hotspot else "REPLACE_PACKAGE")
    return "\n".join(
        [
            "-- profiler_diagnosis: COMMIT or ROLLBACK appears inside a high-frequency loop",
            "-- package: %s" % unit_name,
            "DECLARE",
            "  v_batch_count PLS_INTEGER := 0;",
            "BEGIN",
            "  FOR i IN 1..v_ids.COUNT LOOP",
            "    -- DML work here",
            "    v_batch_count := v_batch_count + 1;",
            "    IF MOD(v_batch_count, 1000) = 0 THEN",
            "      COMMIT;",
            "    END IF;",
            "  END LOOP;",
            "  COMMIT;",
            "END;",
            "/",
        ]
    )


def _build_plsql_cpu_loop_hint_sql(row):
    # type: (Dict[str, Any]) -> str
    hotspot = _get_profiler_hotspot(row)
    unit_name = str(hotspot.get("unit_name") if hotspot else "REPLACE_PACKAGE")
    return "\n".join(
        [
            "-- profiler_diagnosis: tight CPU loop dominates this package block",
            "-- package: %s" % unit_name,
            "-- Replace procedural math/string work with set-based SQL, deterministic helper functions, or cached precomputed values.",
        ]
    )


def _build_diagnosis_recommendations(row):
    # type: (Dict[str, Any]) -> List[Dict[str, str]]
    recommendations = []
    diagnoses = row.get("plsql_profile_diagnoses") or []
    diagnosis_ids = {str(item.get("diagnosis_id") or "") for item in diagnoses}
    if "dynamic_sql_in_loop" in diagnosis_ids:
        recommendations.append(
            {
                "rule_id": "PLSQL-DYNAMIC-SQL",
                "message": "Profiler shows dynamic SQL inside a hot package loop.",
                "hint_sql": _build_plsql_dynamic_sql_hint_sql(row),
            }
        )
    if "frequent_commit_in_loop" in diagnosis_ids:
        recommendations.append(
            {
                "rule_id": "PLSQL-COMMIT-HOT",
                "message": "Profiler shows COMMIT or ROLLBACK inside a high-frequency loop.",
                "hint_sql": _build_plsql_commit_hot_hint_sql(row),
            }
        )
    if "tight_cpu_loop" in diagnosis_ids:
        recommendations.append(
            {
                "rule_id": "PLSQL-CPU-HOT",
                "message": "Profiler shows a tight CPU loop dominating package runtime.",
                "hint_sql": _build_plsql_cpu_loop_hint_sql(row),
            }
        )
    if "bulk_candidate" in diagnosis_ids:
        recommendations.append(
            {
                "rule_id": "PLSQL-BULK",
                "message": "Profiler shows a loop that should be rewritten with BULK COLLECT, FORALL, or MERGE.",
                "hint_sql": _build_plsql_rpc_hint_sql(row),
            }
        )
    return recommendations


def build_recommendations(row, slowdown_threshold):
    # type: (Dict[str, Any], float) -> List[Dict[str, str]]
    recommendations = []
    derived = derive_replay_metrics(row)
    speedup_ratio = row.get("speedup_ratio")
    if speedup_ratio is None:
        speedup_ratio = derived.get("speedup_ratio")
    net_ratio = row.get("net_ratio")
    if net_ratio is None:
        net_ratio = derived.get("net_ratio")
    plan_miss_ratio = row.get("plan_miss_ratio")
    if plan_miss_ratio is None:
        plan_miss_ratio = derived.get("plan_miss_ratio")
    queue_execute_ratio = row.get("queue_execute_ratio")
    if queue_execute_ratio is None:
        queue_execute_ratio = derived.get("queue_execute_ratio")
    lsm_memstore_ratio = row.get("lsm_memstore_ratio")
    if lsm_memstore_ratio is None:
        lsm_memstore_ratio = derived.get("lsm_memstore_ratio")
    sql_text = str(row.get("sql_text") or "")
    if row.get("ob_status") not in (None, "", "ok"):
        if row.get("ob_status") == "skip":
            recommendations.append(
                {
                    "rule_id": "REPLAY-SKIP",
                    "message": "Replay was skipped because the SQL could not be rendered safely",
                    "hint_sql": "-- Review bind values and convert unsupported types before replaying",
                }
            )
            return recommendations
        recommendations.append(
            {
                "rule_id": "REPLAY-ERROR",
                "message": "OceanBase replay did not complete successfully",
                "hint_sql": "-- Inspect obclient stderr and retry the statement in isolation",
            }
        )
    if speedup_ratio is not None and speedup_ratio < float(slowdown_threshold):
        recommendations.append(
            {
                "rule_id": "SLOWDOWN",
                "message": "OceanBase execution is slower than Oracle baseline",
                "hint_sql": "-- Review plan shape, SQL Audit timing breakdown, and index locality",
            }
        )
    if net_ratio is not None and net_ratio > 0.6:
        recommendations.append(
            {
                "rule_id": "DIST-JOIN",
                "message": "Network time dominates execution; distributed execution is likely",
                "hint_sql": _build_dist_join_hint_sql(row),
            }
        )
    if (
        net_ratio is not None
        and net_ratio > 0.6
        and ("BEGIN" in sql_text.upper() or "DECLARE" in sql_text.upper() or "PACKAGE" in sql_text.upper() or "PKG." in sql_text.upper())
        and str(row.get("ob_is_executor_rpc") or "").strip() in ("1", "True", "true", "YES", "yes")
    ):
        hotspot = _get_profiler_hotspot(row)
        message = "PL/SQL or package execution appears to trigger executor RPC overhead"
        if hotspot:
            message = (
                "PL/SQL executor RPC overhead aligns with profiler hotspot at %s line %s"
                % (hotspot.get("unit_name"), hotspot.get("line"))
            )
        recommendations.append(
            {
                "rule_id": "PLSQL-RPC",
                "message": message,
                "hint_sql": _build_plsql_rpc_hint_sql(row),
            }
        )
    if (
        str(row.get("ob_is_hit_plan") or "").strip() in ("0", "False", "false", "NO", "no")
        and plan_miss_ratio is not None
        and plan_miss_ratio > 0.2
    ):
        recommendations.append(
            {
                "rule_id": "PLAN-MISS",
                "message": "Plan acquisition cost is high and plan cache does not appear to be hit",
                "hint_sql": _build_plan_miss_hint_sql(row),
            }
        )
    if queue_execute_ratio is not None and queue_execute_ratio > 3.0 and float(row.get("ob_retry_cnt") or 0.0) > 0.0:
        recommendations.append(
            {
                "rule_id": "LOCK-HOT",
                "message": "Queueing dominates execution and retry count is non-zero, suggesting lock contention",
                "hint_sql": (
                    "-- Investigate SELECT FOR UPDATE hot rows and add NOWAIT or retry backoff where appropriate"
                ),
            }
        )
    if (
        lsm_memstore_ratio is not None
        and lsm_memstore_ratio > 0.7
        and float(row.get("ob_bloom_filter_filtered") or 0.0) <= 0.0
    ):
        recommendations.append(
            {
                "rule_id": "LSM-JITTER",
                "message": "Memstore reads dominate and bloom filtering does not appear to help",
                "hint_sql": (
                    "-- Reduce batch size, inspect compaction timing, and review SSTable pruning effectiveness"
                ),
            }
        )
    if row.get("plan_changed"):
        recommendations.append(
            {
                "rule_id": "PLAN-CHANGED",
                "message": "Oracle and OceanBase plans differ materially",
                "hint_sql": "-- Compare Oracle and OceanBase access paths before changing application SQL",
            }
        )
    if row.get("read_amplification") is not None and row["read_amplification"] > 3.0:
        recommendations.append(
            {
                "rule_id": "READ-AMPLIFICATION",
                "message": "OceanBase logical reads are significantly higher than Oracle",
                "hint_sql": "-- Investigate index usage, partition pruning, and row filtering predicates",
            }
        )
    signal_ids = {str(signal.get("signal_id") or "") for signal in (row.get("plan_diff_signals") or [])}
    if "LOOKUP-RISK" in signal_ids:
        recommendations.append(
            {
                "rule_id": "PLAN-LOOKUP-RISK",
                "message": "Plan translation shows TABLE LOOKUP risk after Oracle indexed row access.",
                "hint_sql": "-- Prefer local indexes or co-located access paths to avoid remote table lookup amplification",
            }
        )
    if "PX-SKEW" in signal_ids:
        recommendations.append(
            {
                "rule_id": "PX-SKEW",
                "message": "Operator output is skewed across workers, indicating parallel imbalance.",
                "hint_sql": "-- Review partitioning and PQ_DISTRIBUTE strategy to reduce worker skew",
            }
        )
    if row.get("plsql_profile_status") == "ok" and row.get("plsql_profile_top_lines"):
        recommendations.append(
            {
                "rule_id": "PLSQL-HOTLINE",
                "message": "Profiler captured a hot PL/SQL source line for this replay.",
                "hint_sql": "-- Review the reported package line and replace row-by-row loops with set-based or FORALL logic",
            }
        )
    recommendations.extend(_build_diagnosis_recommendations(row))
    if row.get("verification_status") == "mismatch":
        recommendations.append(
            {
                "rule_id": "RESULT-MISMATCH",
                "message": "Replay result set differs from the source result set",
                "hint_sql": (
                    "-- Inspect the mismatch artifact and compare semantics, collation, and implicit conversion behavior"
                ),
            }
        )
    if any(float(monitor_row.get("workarea_tempseg") or 0.0) > 0.0 for monitor_row in (row.get("plan_monitor_rows") or [])):
        recommendations.append(
            {
                "rule_id": "PLAN-SPILL",
                "message": "Plan monitor shows temp segment spill during execution",
                "hint_sql": (
                    "-- Review join/hash workarea sizing, data skew, and partition-locality before raising memory limits"
                ),
            }
        )
    return recommendations


def load_workload_index(workload_path):
    # type: (Optional[Union[str, Path]]) -> Dict[str, Dict[str, Any]]
    if not workload_path:
        return {}
    index = {}
    for row in read_jsonl(workload_path):
        sql_id = str(row.get("sql_id") or "")
        if sql_id:
            index[sql_id] = row
    return index


def guess_workload_path_from_replay(replay_path):
    # type: (Union[str, Path]) -> Optional[Path]
    replay_file = Path(replay_path)
    name = replay_file.name
    if not name.startswith("replay_"):
        return None
    candidate = replay_file.with_name("workload_" + name[len("replay_"):])
    if candidate.exists():
        return candidate
    return None


def _escape_obclient_option_value(value):
    # type: (str) -> str
    return str(value or "").replace("\\", "\\\\").replace('"', '\\"')


def ensure_obclient_defaults_file(ob_cfg):
    # type: (Dict[str, str]) -> Path
    cached = str(ob_cfg.get("__ob_defaults_file") or "").strip()
    if cached:
        cached_path = Path(cached)
        if cached_path.exists():
            return cached_path

    temp_dir = str(ob_cfg.get("__temp_dir") or "").strip() or None
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", prefix="perf_obclient_", suffix=".cnf", delete=False, dir=temp_dir
    ) as handle:
        handle.write("[client]\n")
        handle.write(
            'password="%s"\n'
            % _escape_obclient_option_value(str(ob_cfg.get("password", "") or ""))
        )
        temp_path = Path(handle.name)
    os.chmod(str(temp_path), stat.S_IRUSR | stat.S_IWUSR)
    ob_cfg["__ob_defaults_file"] = str(temp_path)
    with _SECURE_FILES_LOCK:
        _SECURE_FILES.add(str(temp_path))
    return temp_path


def cleanup_secure_tempfiles():
    # type: () -> None
    with _SECURE_FILES_LOCK:
        paths = list(_SECURE_FILES)
        _SECURE_FILES.clear()
    for raw_path in paths:
        try:
            Path(raw_path).unlink()
        except OSError:
            pass


atexit.register(cleanup_secure_tempfiles)


def build_obclient_command_args(ob_cfg, extra_args=None):
    # type: (Dict[str, str], Optional[Sequence[str]]) -> List[str]
    defaults_file = ensure_obclient_defaults_file(ob_cfg)
    command_args = [
        str(ob_cfg["executable"]),
        "--defaults-extra-file=%s" % str(defaults_file),
        "-h",
        str(ob_cfg["host"]),
        "-P",
        str(ob_cfg["port"]),
        "-u",
        str(ob_cfg["user_string"]),
    ]
    if extra_args:
        command_args.extend(list(extra_args))
    return command_args


def build_obclient_sql_payload(sql_text, session_query_timeout_us=0):
    # type: (str, int) -> str
    statements = []
    if int(session_query_timeout_us or 0) > 0:
        statements.append("SET ob_query_timeout = %d;" % int(session_query_timeout_us))
    statements.append(str(sql_text).rstrip().rstrip(";") + ";")
    return "\n".join(statements) + "\n"


def obclient_run_sql(ob_cfg, sql_text, timeout=None, session_query_timeout_us=0):
    # type: (Dict[str, str], str, Optional[int], int) -> Tuple[bool, str, str]
    timeout_value = int(timeout or DEFAULT_OBCLIENT_TIMEOUT)
    payload = build_obclient_sql_payload(sql_text, session_query_timeout_us=session_query_timeout_us)
    args = build_obclient_command_args(ob_cfg, extra_args=["-ss"])
    try:
        result = subprocess.run(
            args,
            input=payload,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            timeout=timeout_value,
        )
    except OSError as exc:
        return False, "", str(exc)
    except subprocess.TimeoutExpired as exc:
        return False, exc.stdout or "", "obclient timed out after %ss" % timeout_value
    return result.returncode == 0, (result.stdout or "").strip(), (result.stderr or "").strip()


def validate_runtime_paths(config):
    # type: (AppConfig) -> PreflightResult
    result = PreflightResult()

    if config.oracle_source:
        try:
            parse_oracle_dsn(config.oracle_source.get("dsn", ""))
        except ConfigError as exc:
            result.errors.append(str(exc))

    if config.oceanbase_target:
        obclient_path = Path(config.oceanbase_target.get("executable", "")).expanduser()
        if not obclient_path.exists():
            result.errors.append(
                "obclient executable not found: %s" % str(obclient_path)
            )
        elif not os.access(str(obclient_path), os.X_OK):
            result.warnings.append(
                "obclient executable exists but is not executable: %s" % str(obclient_path)
            )

    if config.settings.get("source_db_mode") == SOURCE_DB_MODE_OCEANBASE:
        source_ob_path = Path(config.oceanbase_source.get("executable", "")).expanduser()
        if not source_ob_path.exists():
            result.errors.append("source obclient executable not found: %s" % str(source_ob_path))
        elif not os.access(str(source_ob_path), os.X_OK):
            result.warnings.append(
                "source obclient executable exists but is not executable: %s" % str(source_ob_path)
            )
        if config.oceanbase_source_sys:
            source_sys_ob_path = Path(
                config.oceanbase_source_sys.get("executable", "")
            ).expanduser()
            if not source_sys_ob_path.exists():
                result.errors.append(
                    "source SYS obclient executable not found: %s" % str(source_sys_ob_path)
                )
            elif not os.access(str(source_sys_ob_path), os.X_OK):
                result.warnings.append(
                    "source SYS obclient executable exists but is not executable: %s"
                    % str(source_sys_ob_path)
                )
        visibility_warning = build_query_sql_visibility_warning(config)
        if visibility_warning:
            result.warnings.append(visibility_warning)

    if oracledb is None and config.oracle_source:
        result.warnings.append(
            "python-oracledb is not installed in the current environment; Oracle capture will not run"
        )

    obdiag_executable = str(config.settings.get("obdiag_executable") or "").strip()
    if obdiag_executable:
        obdiag_path = Path(obdiag_executable).expanduser()
        if not obdiag_path.exists():
            result.warnings.append("obdiag executable not found: %s" % str(obdiag_path))
        elif not os.access(str(obdiag_path), os.X_OK):
            result.warnings.append(
                "obdiag executable exists but is not executable: %s" % str(obdiag_path)
            )

    authorization_env = str(config.settings.get("ocp_authorization_env") or "").strip()
    if authorization_env and not os.environ.get(authorization_env):
        result.warnings.append("OCP authorization env is not set: %s" % authorization_env)

    token_env = str(config.settings.get("ocp_auth_token_env") or "").strip()
    has_ocp_templates = bool(
        str(config.settings.get("ocp_ash_url_template") or "").strip()
        or str(config.settings.get("ocp_qpm_url_template") or "").strip()
    )
    if has_ocp_templates and token_env and not os.environ.get(token_env):
        result.warnings.append("OCP auth token env is not set: %s" % token_env)

    ocp_username = str(config.settings.get("ocp_username") or "").strip()
    ocp_password_env = str(config.settings.get("ocp_password_env") or "").strip()
    ocp_password = str(config.settings.get("ocp_password") or "").strip()
    if ocp_username and not (ocp_password or (ocp_password_env and os.environ.get(ocp_password_env))):
        result.warnings.append("OCP username is set but password or password env is missing")

    return result


def summarize_config(config):
    # type: (AppConfig) -> Dict[str, Any]
    summary = {
        "config_path": config.config_path,
        "settings": config.settings,
    }
    if config.oceanbase_target:
        summary["oceanbase_target"] = {
            "executable": config.oceanbase_target["executable"],
            "host": config.oceanbase_target["host"],
            "port": config.oceanbase_target["port"],
            "user_string": config.oceanbase_target["user_string"],
        }
    if config.oracle_source:
        host, port, service_name = parse_oracle_dsn(config.oracle_source["dsn"])
        summary["oracle_source"] = {
            "user": config.oracle_source["user"],
            "host": host,
            "port": port,
            "service_name": service_name,
        }
    if config.oceanbase_source:
        summary["oceanbase_source"] = {
            "executable": config.oceanbase_source["executable"],
            "host": config.oceanbase_source["host"],
            "port": config.oceanbase_source["port"],
            "user_string": config.oceanbase_source["user_string"],
        }
    if config.oceanbase_source_sys:
        summary["oceanbase_source_sys"] = {
            "executable": config.oceanbase_source_sys["executable"],
            "host": config.oceanbase_source_sys["host"],
            "port": config.oceanbase_source_sys["port"],
            "user_string": config.oceanbase_source_sys["user_string"],
        }
    return summary


def _print_preflight(result):
    # type: (PreflightResult) -> None
    for message in result.warnings:
        LOG.warning(message)
    for message in result.errors:
        LOG.error(message)


def probe_oracle_capabilities(config):
    # type: (AppConfig) -> Dict[str, Any]
    capabilities = {
        "oracle_driver_available": bool(oracledb is not None),
        "configured": bool(config.oracle_source),
        "awr": False,
        "vsql": False,
        "unified_audit": False,
        "wcr": bool(str(config.settings.get("wcr_path") or "").strip()),
        "sql_file": True,
    }
    if oracledb is None or not config.oracle_source:
        return capabilities
    try:
        connection = oracledb.connect(
            user=config.oracle_source["user"],
            password=config.oracle_source["password"],
            dsn=config.oracle_source["dsn"],
        )
    except Exception as exc:
        capabilities["error"] = str(exc)
        return capabilities
    try:
        cursor = connection.cursor()
        probes = {
            "awr": "SELECT COUNT(*) FROM DBA_HIST_SQLSTAT WHERE ROWNUM = 1",
            "vsql": "SELECT COUNT(*) FROM V$SQL WHERE ROWNUM = 1",
            "unified_audit": "SELECT COUNT(*) FROM UNIFIED_AUDIT_TRAIL WHERE ROWNUM = 1",
        }
        for key, query in probes.items():
            try:
                cursor.execute(query)
                cursor.fetchone()
                capabilities[key] = True
            except Exception:
                capabilities[key] = False
    finally:
        try:
            connection.close()
        except Exception:
            pass
    return capabilities


def probe_replay_capabilities(config):
    # type: (AppConfig) -> Dict[str, Any]
    if not config.oceanbase_target:
        return {
            "obclient": False,
            "sql_audit": False,
            "explain": False,
            "configured": False,
            "plsql_profiler": {"available": False, "status": "unconfigured", "error": ""},
            "ocp": _probe_ocp_capability(config),
            "obdiag": _probe_obdiag_capability(config),
        }
    ob_cfg = config.oceanbase_target
    capabilities = {
        "obclient": Path(ob_cfg["executable"]).exists(),
        "sql_audit": False,
        "explain": False,
        "plsql_profiler": {"available": False, "status": "unavailable", "error": ""},
        "ocp": _probe_ocp_capability(config),
        "obdiag": _probe_obdiag_capability(config),
    }
    if not capabilities["obclient"]:
        return capabilities
    ok, stdout, stderr = obclient_run_sql(
        ob_cfg,
        "SELECT 1 FROM DUAL",
        timeout=config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT),
        session_query_timeout_us=config.settings.get(
            "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
        ),
    )
    capabilities["connectivity_ok"] = ok
    if not ok:
        capabilities["error"] = stderr or stdout
        return capabilities
    capabilities["explain"] = True
    ok, stdout, stderr = obclient_run_sql(
        ob_cfg,
        (
            "SELECT VALUE FROM GV$OB_PARAMETERS "
            "WHERE NAME = 'ob_enable_sql_audit' AND ROWNUM = 1"
        ),
        timeout=config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT),
        session_query_timeout_us=config.settings.get(
            "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
        ),
    )
    capabilities["sql_audit"] = ok and "OFF" not in (stdout or "").upper()
    if stderr and not ok:
        capabilities["sql_audit_error"] = stderr
    capabilities["plsql_profiler"] = _probe_plsql_profiler_capability(config)
    return capabilities


def write_capability_files(config, run_id, capture_capabilities=None, replay_capabilities=None):
    # type: (AppConfig, str) -> None
    workloads_dir = config.settings["workloads_dir"]
    default_capture_capabilities = {
        "run_id": run_id,
        "oracle_driver_available": bool(oracledb is not None),
    }
    if config.oracle_source:
        default_capture_capabilities["oracle_dsn"] = config.oracle_source["dsn"]
    elif config.oceanbase_source:
        default_capture_capabilities["source_mode"] = SOURCE_DB_MODE_OCEANBASE
        default_capture_capabilities["source_obclient_host"] = config.oceanbase_source.get("host")
    write_json(
        build_artifact_path("capture_capability", run_id, root_dir=workloads_dir),
        capture_capabilities or default_capture_capabilities,
    )
    write_json(
        build_artifact_path("replay_capability", run_id, root_dir=workloads_dir),
        replay_capabilities
        or {
            "run_id": run_id,
            "obclient_executable": config.oceanbase_target.get("executable"),
            "obclient_host": config.oceanbase_target.get("host"),
        },
    )


def capture_from_sql_file(config, sql_file, run_id):
    # type: (AppConfig, str, str) -> Path
    path = Path(sql_file)
    if not path.exists():
        raise ConfigError("SQL file does not exist: %s" % str(path))
    statements = split_sql_text(path.read_text(encoding="utf-8"))
    if not statements:
        raise ConfigError("SQL file did not contain any executable statements")
    workload_rows = []
    default_schema = config.settings["source_schemas"][0]
    for statement in statements:
        normalized = normalize_sql_text(statement)
        workload_rows.append(
            {
                "sql_id": compute_sql_id(statement),
                "sql_text": statement,
                "sql_text_normalized": normalized,
                "bind_vars": {},
                "schema": default_schema,
                "source": "sql_file",
                "captured_at": utc_now_iso(),
                "baseline_source_mode": SOURCE_DB_MODE_ORACLE,
                "baseline_avg_elapsed_us": None,
                "baseline_avg_logical_reads": None,
                "oracle_executions": 1,
                "oracle_avg_elapsed_us": None,
                "oracle_avg_cpu_us": None,
                "oracle_avg_logical_reads": None,
                "oracle_avg_physical_reads": None,
                "oracle_plan_hash": None,
                "oracle_plan_rows": [],
            }
        )
    workload_path = build_artifact_path("workload", run_id, root_dir=config.settings["workloads_dir"])
    append_jsonl(workload_path, workload_rows)
    return workload_path


def _capture_from_unified_audit(connection, config):
    # type: (Any, AppConfig) -> List[Dict[str, Any]]
    schemas = config.settings["source_schemas"]
    placeholders, binds = _oracle_schema_placeholders("schema", schemas)
    binds.update({"hours": int(config.settings["hours"]), "top_n": get_capture_top_n(config)})
    query = """
        SELECT * FROM (
          SELECT
            SQL_TEXT,
            DBUSERNAME,
            COUNT(*) AS EXECUTIONS,
            TO_CHAR(MAX(EVENT_TIMESTAMP), 'YYYY-MM-DD"T"HH24:MI:SS') AS CAPTURED_AT
          FROM UNIFIED_AUDIT_TRAIL
          WHERE DBUSERNAME IN ({placeholders})
            AND SQL_TEXT IS NOT NULL
            AND EVENT_TIMESTAMP >= SYSTIMESTAMP - NUMTODSINTERVAL(:hours, 'HOUR')
          GROUP BY SQL_TEXT, DBUSERNAME
          ORDER BY EXECUTIONS DESC, CAPTURED_AT DESC
        ) WHERE ROWNUM <= :top_n
    """.format(placeholders=placeholders)
    cursor = connection.cursor()
    cursor.execute(query, binds)
    rows = []
    for row in cursor:
        sql_text = _coerce_jsonable(row[0])
        sql_id = compute_sql_id(sql_text)
        rows.append(
            {
                "sql_id": sql_id,
                "sql_text": sql_text,
                "sql_text_normalized": normalize_sql_text(sql_text),
                "bind_vars": {},
                "schema": _coerce_jsonable(row[1]),
                "source": "unified_audit",
                "captured_at": _coerce_jsonable(row[3]) or utc_now_iso(),
                "baseline_source_mode": SOURCE_DB_MODE_ORACLE,
                "baseline_avg_elapsed_us": None,
                "baseline_avg_logical_reads": None,
                "oracle_executions": int(row[2]) if row[2] is not None else 0,
                "oracle_avg_elapsed_us": None,
                "oracle_avg_cpu_us": None,
                "oracle_avg_logical_reads": None,
                "oracle_avg_physical_reads": None,
                "oracle_plan_hash": None,
                "oracle_plan_rows": [],
            }
        )
    cursor.close()
    return rows


def _normalize_wcr_item(item, default_schema):
    # type: (Dict[str, Any], str) -> Optional[Dict[str, Any]]
    sql_text = _coerce_jsonable(
        item.get("sql_text") or item.get("sql") or item.get("statement") or item.get("query")
    )
    if not str(sql_text or "").strip():
        return None
    captured_at = _coerce_jsonable(item.get("captured_at") or item.get("timestamp") or utc_now_iso())
    schema_name = _coerce_jsonable(item.get("schema") or item.get("parsing_schema") or default_schema)
    executions = _safe_int(item.get("executions") or item.get("execution_count") or 1) or 1
    avg_elapsed = _safe_float(item.get("avg_elapsed_us") or item.get("elapsed_us"))
    avg_logical_reads = _safe_float(item.get("avg_logical_reads") or item.get("logical_reads"))
    avg_physical_reads = _safe_float(item.get("avg_physical_reads") or item.get("physical_reads"))
    plan_hash = _coerce_jsonable(item.get("oracle_plan_hash") or item.get("plan_hash"))
    return {
        "sql_id": _coerce_jsonable(item.get("sql_id")) or compute_sql_id(sql_text),
        "sql_text": sql_text,
        "sql_text_normalized": normalize_sql_text(sql_text),
        "bind_vars": item.get("bind_vars") or {},
        "schema": schema_name,
        "source": "wcr",
        "captured_at": captured_at,
        "baseline_source_mode": SOURCE_DB_MODE_ORACLE,
        "baseline_avg_elapsed_us": avg_elapsed,
        "baseline_avg_logical_reads": avg_logical_reads,
        "oracle_executions": executions,
        "oracle_avg_elapsed_us": avg_elapsed,
        "oracle_avg_cpu_us": _safe_float(item.get("avg_cpu_us")),
        "oracle_avg_logical_reads": avg_logical_reads,
        "oracle_avg_physical_reads": avg_physical_reads,
        "oracle_plan_hash": plan_hash,
        "oracle_plan_rows": item.get("oracle_plan_rows") or [],
    }


def _parse_wcr_json_payload(payload_text, default_schema):
    # type: (str, str) -> List[Dict[str, Any]]
    try:
        payload = json.loads(payload_text)
    except Exception:
        return []
    if isinstance(payload, dict):
        items = payload.get("statements") or payload.get("rows") or payload.get("items") or []
    elif isinstance(payload, list):
        items = payload
    else:
        return []
    rows = []
    for item in items:
        if not isinstance(item, dict):
            continue
        normalized = _normalize_wcr_item(item, default_schema)
        if normalized:
            rows.append(normalized)
    return rows


def _parse_wcr_jsonl_payload(payload_text, default_schema):
    # type: (str, str) -> List[Dict[str, Any]]
    rows = []
    for line in str(payload_text or "").splitlines():
        raw_line = line.strip()
        if not raw_line:
            continue
        try:
            item = json.loads(raw_line)
        except Exception:
            return []
        if not isinstance(item, dict):
            continue
        normalized = _normalize_wcr_item(item, default_schema)
        if normalized:
            rows.append(normalized)
    return rows


def _parse_wcr_xml_payload(payload_text, default_schema):
    # type: (str, str) -> List[Dict[str, Any]]
    try:
        root = ET.fromstring(payload_text)
    except Exception:
        return []
    rows = []
    candidate_nodes = root.findall(".//statement") + root.findall(".//sql")
    for node in candidate_nodes:
        item = {
            "sql_text": (node.findtext("sql_text") or node.findtext("text") or node.text or "").strip(),
            "schema": (node.findtext("schema") or node.findtext("dbusername") or default_schema),
            "captured_at": node.findtext("captured_at") or node.findtext("timestamp") or utc_now_iso(),
            "executions": node.findtext("executions") or "1",
        }
        normalized = _normalize_wcr_item(item, default_schema)
        if normalized:
            rows.append(normalized)
    return rows


def parse_wcr_payload(payload_text, default_schema):
    # type: (str, str) -> List[Dict[str, Any]]
    payload_text = str(payload_text or "")
    parsers = [
        _parse_wcr_json_payload,
        _parse_wcr_jsonl_payload,
        _parse_wcr_xml_payload,
    ]
    for parser in parsers:
        rows = parser(payload_text, default_schema)
        if rows:
            return rows
    rows = []
    for statement in split_sql_text(payload_text):
        statement = statement.strip()
        if not statement:
            continue
        rows.append(
            {
                "sql_id": compute_sql_id(statement),
                "sql_text": statement,
                "sql_text_normalized": normalize_sql_text(statement),
                "bind_vars": {},
                "schema": default_schema,
                "source": "wcr",
                "captured_at": utc_now_iso(),
                "baseline_source_mode": SOURCE_DB_MODE_ORACLE,
                "baseline_avg_elapsed_us": None,
                "baseline_avg_logical_reads": None,
                "oracle_executions": 1,
                "oracle_avg_elapsed_us": None,
                "oracle_avg_cpu_us": None,
                "oracle_avg_logical_reads": None,
                "oracle_avg_physical_reads": None,
                "oracle_plan_hash": None,
                "oracle_plan_rows": [],
            }
        )
    return rows


def capture_from_wcr_file(config, wcr_path, run_id):
    # type: (AppConfig, str, str) -> Path
    path = Path(str(wcr_path or "")).expanduser()
    if not path.exists():
        raise ConfigError("WCR file does not exist: %s" % str(path))
    payload_text = path.read_text(encoding="utf-8", errors="ignore")
    rows = parse_wcr_payload(payload_text, config.settings["source_schemas"][0])
    if not rows:
        raise ConfigError("WCR input did not produce any SQL statements: %s" % str(path))
    workload_path = build_artifact_path("workload", run_id, root_dir=config.settings["workloads_dir"])
    append_jsonl(workload_path, rows)
    return workload_path


def _open_oracle_connection(config):
    # type: (AppConfig) -> Any
    if oracledb is None:
        raise ConfigError("python-oracledb is not installed")
    if not config.oracle_source:
        raise ConfigError("Oracle source is not configured for this workflow")
    return oracledb.connect(
        user=config.oracle_source["user"],
        password=config.oracle_source["password"],
        dsn=config.oracle_source["dsn"],
    )


def _oracle_schema_placeholders(prefix, schemas):
    # type: (str, List[str]) -> Tuple[str, Dict[str, Any]]
    placeholders = []
    binds = {}
    for idx, schema in enumerate(schemas):
        key = "%s%d" % (prefix, idx)
        placeholders.append(":" + key)
        binds[key] = schema
    return ", ".join(placeholders), binds


def _read_oracle_plan_rows(connection, sql_id, use_awr=False):
    # type: (Any, str, bool) -> List[Dict[str, Any]]
    view_name = "DBA_HIST_SQL_PLAN" if use_awr else "V$SQL_PLAN"
    query = (
        "SELECT ID, OPERATION, OPTIONS, OBJECT_NAME "
        "FROM %s WHERE SQL_ID = :sql_id ORDER BY ID" % view_name
    )
    cursor = connection.cursor()
    try:
        cursor.execute(query, sql_id=sql_id)
        rows = []
        for row in cursor:
            rows.append(
                {
                    "id": int(row[0]) if row[0] is not None else None,
                    "operation": _coerce_jsonable(row[1]),
                    "options": _coerce_jsonable(row[2]),
                    "object_name": _coerce_jsonable(row[3]),
                }
            )
        return rows
    except Exception:
        return []
    finally:
        cursor.close()


def _capture_from_vsql(connection, config, since_ts=None):
    # type: (Any, AppConfig, Optional[str]) -> List[Dict[str, Any]]
    schemas = config.settings["source_schemas"]
    placeholders, binds = _oracle_schema_placeholders("schema", schemas)
    binds.update(
        {
            "hours": int(config.settings["hours"]),
            "min_exec": int(config.settings["min_exec"]),
            "top_n": get_capture_top_n(config),
        }
    )
    time_filter = (
        "AND LAST_ACTIVE_TIME > TO_TIMESTAMP(:since_ts, 'YYYY-MM-DD\"T\"HH24:MI:SS')"
        if since_ts
        else "AND LAST_ACTIVE_TIME >= SYSTIMESTAMP - NUMTODSINTERVAL(:hours, 'HOUR')"
    )
    if since_ts:
        binds["since_ts"] = since_ts
    query = """
        SELECT * FROM (
          SELECT
            SQL_ID,
            SQL_FULLTEXT,
            PARSING_SCHEMA_NAME,
            EXECUTIONS,
            ROUND(ELAPSED_TIME / DECODE(EXECUTIONS, 0, 1, EXECUTIONS)) AS AVG_ELAPSED_US,
            ROUND(CPU_TIME / DECODE(EXECUTIONS, 0, 1, EXECUTIONS)) AS AVG_CPU_US,
            ROUND(BUFFER_GETS / DECODE(EXECUTIONS, 0, 1, EXECUTIONS)) AS AVG_LOGICAL_READS,
            ROUND(DISK_READS / DECODE(EXECUTIONS, 0, 1, EXECUTIONS)) AS AVG_PHYSICAL_READS,
            PLAN_HASH_VALUE,
            TO_CHAR(LAST_ACTIVE_TIME, 'YYYY-MM-DD"T"HH24:MI:SS') AS CAPTURED_AT
          FROM V$SQL
          WHERE PARSING_SCHEMA_NAME IN ({placeholders})
            AND EXECUTIONS >= :min_exec
            {time_filter}
          ORDER BY AVG_ELAPSED_US DESC
        ) WHERE ROWNUM <= :top_n
    """.format(placeholders=placeholders, time_filter=time_filter)
    cursor = connection.cursor()
    cursor.execute(query, binds)
    rows = []
    for row in cursor:
        sql_text = _coerce_jsonable(row[1])
        sql_id = _coerce_jsonable(row[0]) or compute_sql_id(sql_text)
        rows.append(
            {
                "sql_id": sql_id,
                "sql_text": sql_text,
                "sql_text_normalized": normalize_sql_text(sql_text),
                "bind_vars": {},
                "schema": _coerce_jsonable(row[2]),
                "source": "vsql",
                "captured_at": _coerce_jsonable(row[9]) or utc_now_iso(),
                "baseline_source_mode": SOURCE_DB_MODE_ORACLE,
                "baseline_avg_elapsed_us": float(row[4]) if row[4] is not None else None,
                "baseline_avg_logical_reads": float(row[6]) if row[6] is not None else None,
                "oracle_executions": int(row[3]) if row[3] is not None else 0,
                "oracle_avg_elapsed_us": float(row[4]) if row[4] is not None else None,
                "oracle_avg_cpu_us": float(row[5]) if row[5] is not None else None,
                "oracle_avg_logical_reads": float(row[6]) if row[6] is not None else None,
                "oracle_avg_physical_reads": float(row[7]) if row[7] is not None else None,
                "oracle_plan_hash": _coerce_jsonable(row[8]),
                "oracle_plan_rows": _read_oracle_plan_rows(connection, sql_id, use_awr=False),
            }
        )
    cursor.close()
    return rows


def _capture_from_awr(connection, config):
    # type: (Any, AppConfig) -> List[Dict[str, Any]]
    schemas = config.settings["source_schemas"]
    placeholders, binds = _oracle_schema_placeholders("schema", schemas)
    binds.update(
        {
            "hours": int(config.settings["hours"]),
            "min_exec": int(config.settings["min_exec"]),
            "top_n": get_capture_top_n(config),
        }
    )
    query = """
        SELECT * FROM (
          SELECT
            ST.SQL_ID,
            TXT.SQL_TEXT,
            ST.PARSING_SCHEMA_NAME,
            SUM(ST.EXECUTIONS_DELTA) AS EXECUTIONS,
            ROUND(SUM(ST.ELAPSED_TIME_DELTA) / DECODE(SUM(ST.EXECUTIONS_DELTA), 0, 1, SUM(ST.EXECUTIONS_DELTA))) AS AVG_ELAPSED_US,
            ROUND(SUM(ST.CPU_TIME_DELTA) / DECODE(SUM(ST.EXECUTIONS_DELTA), 0, 1, SUM(ST.EXECUTIONS_DELTA))) AS AVG_CPU_US,
            ROUND(SUM(ST.BUFFER_GETS_DELTA) / DECODE(SUM(ST.EXECUTIONS_DELTA), 0, 1, SUM(ST.EXECUTIONS_DELTA))) AS AVG_LOGICAL_READS,
            ROUND(SUM(ST.DISK_READS_DELTA) / DECODE(SUM(ST.EXECUTIONS_DELTA), 0, 1, SUM(ST.EXECUTIONS_DELTA))) AS AVG_PHYSICAL_READS,
            MAX(ST.PLAN_HASH_VALUE) AS PLAN_HASH_VALUE,
            TO_CHAR(MAX(SN.END_INTERVAL_TIME), 'YYYY-MM-DD"T"HH24:MI:SS') AS CAPTURED_AT
          FROM DBA_HIST_SQLSTAT ST
          JOIN DBA_HIST_SNAPSHOT SN
            ON SN.DBID = ST.DBID
           AND SN.INSTANCE_NUMBER = ST.INSTANCE_NUMBER
           AND SN.SNAP_ID = ST.SNAP_ID
          JOIN DBA_HIST_SQLTEXT TXT
            ON TXT.DBID = ST.DBID
           AND TXT.SQL_ID = ST.SQL_ID
          WHERE ST.PARSING_SCHEMA_NAME IN ({placeholders})
            AND SN.END_INTERVAL_TIME >= SYSTIMESTAMP - NUMTODSINTERVAL(:hours, 'HOUR')
          GROUP BY ST.SQL_ID, TXT.SQL_TEXT, ST.PARSING_SCHEMA_NAME
          HAVING SUM(ST.EXECUTIONS_DELTA) >= :min_exec
          ORDER BY AVG_ELAPSED_US DESC
        ) WHERE ROWNUM <= :top_n
    """.format(placeholders=placeholders)
    cursor = connection.cursor()
    cursor.execute(query, binds)
    rows = []
    for row in cursor:
        sql_text = _coerce_jsonable(row[1])
        sql_id = _coerce_jsonable(row[0]) or compute_sql_id(sql_text)
        rows.append(
            {
                "sql_id": sql_id,
                "sql_text": sql_text,
                "sql_text_normalized": normalize_sql_text(sql_text),
                "bind_vars": {},
                "schema": _coerce_jsonable(row[2]),
                "source": "awr",
                "captured_at": _coerce_jsonable(row[9]) or utc_now_iso(),
                "baseline_source_mode": SOURCE_DB_MODE_ORACLE,
                "baseline_avg_elapsed_us": float(row[4]) if row[4] is not None else None,
                "baseline_avg_logical_reads": float(row[6]) if row[6] is not None else None,
                "oracle_executions": int(row[3]) if row[3] is not None else 0,
                "oracle_avg_elapsed_us": float(row[4]) if row[4] is not None else None,
                "oracle_avg_cpu_us": float(row[5]) if row[5] is not None else None,
                "oracle_avg_logical_reads": float(row[6]) if row[6] is not None else None,
                "oracle_avg_physical_reads": float(row[7]) if row[7] is not None else None,
                "oracle_plan_hash": _coerce_jsonable(row[8]),
                "oracle_plan_rows": _read_oracle_plan_rows(connection, sql_id, use_awr=True),
            }
        )
    cursor.close()
    return rows


def _build_source_ob_audit_query(last_request_id, include_caller_fields=True):
    # type: (int, bool) -> str
    extra_fields = []
    if include_caller_fields:
        extra_fields = [
            "TENANT_NAME",
            "DB_NAME",
            "USER_NAME",
            "USER_CLIENT_IP",
            "CLIENT_IP",
            "RET_CODE",
        ]
    extra_fields_sql = "".join("\n          , %s" % field for field in extra_fields)
    return """
        SELECT /* perf_comparator_source_poll */
          REQUEST_ID,
          TRACE_ID,
          SQL_ID,
          ELAPSED_TIME,
          QUEUE_TIME,
          GET_PLAN_TIME,
          EXECUTE_TIME,
          NET_TIME,
          NET_WAIT_TIME,
          PLAN_TYPE,
          IS_HIT_PLAN,
          IS_EXECUTOR_RPC,
          TABLE_SCAN,
          RETRY_CNT,
          ROW_CACHE_HIT,
          BLOCK_CACHE_HIT,
          MEMSTORE_READ_ROW_COUNT,
          SSSTORE_READ_ROW_COUNT,
          RETURN_ROWS,
          DISK_READS,
          QUERY_SQL{extra_fields}
        FROM GV$OB_SQL_AUDIT
        WHERE REQUEST_ID > {last_request_id}
        ORDER BY REQUEST_ID
    """.format(last_request_id=int(last_request_id), extra_fields=extra_fields_sql)


def _obclient_run_sql_on_source(config, sql_text, timeout=None, session_query_timeout_us=0):
    # type: (AppConfig, str, Optional[int], int) -> Tuple[bool, str, str]
    return obclient_run_sql(
        config.oceanbase_source,
        sql_text,
        timeout=timeout,
        session_query_timeout_us=session_query_timeout_us,
    )


def get_source_max_request_id(config):
    # type: (AppConfig) -> int
    ok, stdout, _ = _obclient_run_sql_on_source(
        config,
        "SELECT /* perf_comparator_source_seed */ NVL(MAX(REQUEST_ID), 0) FROM GV$OB_SQL_AUDIT",
        timeout=config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT),
        session_query_timeout_us=config.settings.get(
            "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
        ),
    )
    if not ok or not stdout.strip():
        return 0
    return int(_safe_int(stdout.splitlines()[0].split("\t")[0]) or 0)


def capture_workload_from_ob_source(config, args, run_id):
    # type: (AppConfig, argparse.Namespace, str) -> Path
    replay_capabilities = probe_replay_capabilities(config)
    replay_capabilities["run_id"] = run_id
    capture_capabilities = {
        "run_id": run_id,
        "source_mode": SOURCE_DB_MODE_OCEANBASE,
        "source_obclient_executable": config.oceanbase_source.get("executable"),
        "source_obclient_host": config.oceanbase_source.get("host"),
        "sql_audit": True,
    }
    write_capability_files(config, run_id, capture_capabilities, replay_capabilities)
    workload_path = build_artifact_path("workload", run_id, root_dir=config.settings["workloads_dir"])
    duration = int(getattr(args, "duration", 0) or config.settings.get("duration", 0) or (int(config.settings.get("hours", DEFAULT_HOURS)) * 3600))
    duration = max(1, duration)
    poll_interval = max(1, int(config.settings.get("interval", DEFAULT_INTERVAL)))
    started = time.time()
    window_started_at = datetime.now(timezone.utc)
    last_request_id = 0
    last_report_refresh_at = 0.0
    captured_sql_ids = set()
    source_sqlstat_start = {}
    query_profile = str(config.settings.get("_source_audit_query_profile") or "rich").strip() or "rich"
    if getattr(args, "mode", None) == MODE_SOURCE_REPORT:
        source_sqlstat_start = collect_source_sqlstat_snapshot(config)
        last_request_id = get_source_max_request_id(config)
    default_schema = config.settings["source_schemas"][0]
    while True:
        include_caller_fields = query_profile != "legacy"
        ok, stdout, stderr = _obclient_run_sql_on_source(
            config,
            _build_source_ob_audit_query(last_request_id, include_caller_fields=include_caller_fields),
            timeout=config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT),
            session_query_timeout_us=config.settings.get(
                "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
            ),
        )
        if not ok:
            if include_caller_fields and ("ORA-00904" in str(stderr or stdout).upper() or "UNKNOWN COLUMN" in str(stderr or stdout).upper()):
                LOG.warning("Source audit caller fields unavailable; falling back to legacy audit query")
                query_profile = "legacy"
                config.settings["_source_audit_query_profile"] = "legacy"
                continue
            if time.time() - started >= duration:
                break
            LOG.warning("OceanBase source audit polling failed: %s", stderr or stdout)
            time.sleep(poll_interval)
            continue
        rows = parse_ob_audit_rows(stdout, default_schema, captured_at=utc_now_iso())
        if rows:
            append_jsonl(workload_path, rows)
            captured_sql_ids.update(str(row.get("sql_id") or "") for row in rows if row.get("sql_id"))
            last_request_id = max(int(row.get("source_ob_request_id") or 0) for row in rows)
            if getattr(args, "mode", None) == MODE_SOURCE_REPORT:
                last_report_refresh_at = maybe_refresh_source_report(
                    config, workload_path, run_id, last_report_refresh_at
                )
        if time.time() - started >= duration:
            break
        time.sleep(poll_interval)
    if getattr(args, "mode", None) == MODE_SOURCE_REPORT and source_sqlstat_start:
        source_sqlstat_end = collect_source_sqlstat_snapshot(config)
        sqlstat_rows = build_source_sqlstat_delta_rows(
            source_sqlstat_start,
            source_sqlstat_end,
            captured_sql_ids=captured_sql_ids,
            default_schema=default_schema,
            captured_at=utc_now_iso(),
        )
        if sqlstat_rows:
            append_jsonl(workload_path, sqlstat_rows)
            captured_sql_ids.update(str(row.get("sql_id") or "") for row in sqlstat_rows if row.get("sql_id"))
        plan_cache_recent = collect_source_plan_cache_recent_rows(config, window_started_at)
        plan_cache_rows = build_source_plan_cache_recent_rows(
            plan_cache_recent,
            captured_sql_ids=captured_sql_ids,
            default_schema=default_schema,
            captured_at=utc_now_iso(),
        )
        if plan_cache_rows:
            append_jsonl(workload_path, plan_cache_rows)
        last_report_refresh_at = maybe_refresh_source_report(
            config, workload_path, run_id, last_report_refresh_at, force=True
        )
    if not workload_path.exists():
        raise ConfigError("OceanBase source capture did not produce any workload rows")
    return workload_path


def aggregate_ob_source_workload_rows(workload_rows):
    # type: (List[Dict[str, Any]]) -> List[Dict[str, Any]]
    grouped = {}  # type: Dict[str, Dict[str, Any]]
    source_priority = {
        "captured": 4,
        "source_sys": 3,
        "ocp_native": 2,
        "ocp_template": 1,
        "missing": 0,
    }
    for row in workload_rows:
        sql_text = str(row.get("sql_text") or "")
        sample_increment = max(1, int(_safe_int(row.get("source_execution_count")) or 1))
        total_elapsed_us = _safe_float(row.get("source_total_elapsed_us"))
        if total_elapsed_us is None:
            total_elapsed_us = (
                float(row.get("baseline_avg_elapsed_us") or row.get("oracle_avg_elapsed_us") or 0.0)
                * sample_increment
            )
        actor_fields = row.get("source_actor_fields") or list(DEFAULT_SOURCE_ACTOR_FIELDS)
        actor_key = build_source_actor_key(row, actor_fields, allow_fallback=False)
        fallback_actor_key = build_source_fallback_actor_key(row)
        workload_type = classify_source_workload_type(sql_text)
        key = str(row.get("sql_id") or compute_sql_id(sql_text))
        entry = grouped.get(key)
        if entry is None:
            entry = {
                "sql_id": key,
                "sql_text": sql_text,
                "sql_text_normalized": row.get("sql_text_normalized") or normalize_sql_text(sql_text),
                "schema": row.get("schema"),
                "source": "ob_source_report",
                "ob_status": "ok",
                "baseline_source_mode": SOURCE_DB_MODE_OCEANBASE,
                "source_sample_count": 0,
                "source_total_elapsed_us": 0.0,
                "source_total_logical_reads": 0.0,
                "source_total_physical_reads": 0.0,
                "source_total_queue_time_us": 0.0,
                "source_total_get_plan_time_us": 0.0,
                "source_total_execute_time_us": 0.0,
                "source_total_net_time_us": 0.0,
                "source_total_net_wait_time_us": 0.0,
                "source_total_retry_cnt": 0.0,
                "source_total_memstore_read_rows": 0.0,
                "source_total_ssstore_read_rows": 0.0,
                "source_total_bloom_filter_filtered": 0.0,
                "source_plan_miss_count": 0,
                "source_rpc_count": 0,
                "source_ob_trace_id": row.get("source_ob_trace_id"),
                "source_ob_request_id": row.get("source_ob_request_id"),
                "source_ob_plan_type_raw": row.get("source_ob_plan_type_raw"),
                "captured_at": row.get("captured_at"),
                "source_sql_text_source": row.get("source_sql_text_source") or row.get("source_sql_text_status"),
                "source_actor_counts": {},
                "source_fallback_actor_counts": {},
                "source_actor_fields": list(actor_fields),
                "source_workload_type": workload_type,
                "source_tenant_name": row.get("source_tenant_name"),
                "source_db_name": row.get("source_db_name"),
                "source_user_name": row.get("source_user_name"),
                "source_user_client_ip": row.get("source_user_client_ip"),
                "source_client_ip": row.get("source_client_ip"),
                "source_ret_code": row.get("source_ret_code"),
                "source_first_seen_at": row.get("captured_at"),
                "source_last_seen_at": row.get("captured_at"),
                "plsql_profile_status": row.get("plsql_profile_status"),
                "plsql_profile_summary": row.get("plsql_profile_summary"),
                "plsql_profile_mapping_summary": row.get("plsql_profile_mapping_summary"),
                "plsql_profile_diagnosis_summary": row.get("plsql_profile_diagnosis_summary"),
                "plsql_profile_diagnoses": row.get("plsql_profile_diagnoses") or [],
            }
            grouped[key] = entry
        entry["source_sample_count"] += sample_increment
        entry["source_total_elapsed_us"] += float(total_elapsed_us or 0.0)
        entry["source_total_logical_reads"] += float(
            row.get("source_ob_logical_reads")
            or row.get("baseline_avg_logical_reads")
            or row.get("oracle_avg_logical_reads")
            or 0.0
        ) * sample_increment
        entry["source_total_physical_reads"] += float(
            row.get("source_ob_physical_reads") or row.get("oracle_avg_physical_reads") or 0.0
        ) * sample_increment
        entry["source_total_queue_time_us"] += float(row.get("source_ob_queue_time_us") or 0.0) * sample_increment
        entry["source_total_get_plan_time_us"] += float(row.get("source_ob_get_plan_time_us") or 0.0) * sample_increment
        entry["source_total_execute_time_us"] += float(row.get("source_ob_execute_time_us") or 0.0) * sample_increment
        entry["source_total_net_time_us"] += float(row.get("source_ob_net_time_us") or 0.0) * sample_increment
        entry["source_total_net_wait_time_us"] += float(row.get("source_ob_net_wait_time_us") or 0.0) * sample_increment
        entry["source_total_retry_cnt"] += float(row.get("source_ob_retry_cnt") or 0.0) * sample_increment
        entry["source_total_memstore_read_rows"] += float(row.get("source_ob_memstore_read_rows") or 0.0) * sample_increment
        entry["source_total_ssstore_read_rows"] += float(row.get("source_ob_ssstore_read_rows") or 0.0) * sample_increment
        entry["source_total_bloom_filter_filtered"] += float(row.get("source_ob_bloom_filter_filtered") or 0.0) * sample_increment
        entry["source_plan_miss_count"] += 0 if _is_truthy_flag(row.get("source_ob_is_hit_plan")) else sample_increment
        entry["source_rpc_count"] += sample_increment if _is_truthy_flag(row.get("source_ob_is_executor_rpc")) else 0
        if actor_key:
            entry["source_actor_counts"][actor_key] = int(entry["source_actor_counts"].get(actor_key, 0) or 0) + sample_increment
        else:
            entry["source_fallback_actor_counts"][fallback_actor_key] = int(
                entry["source_fallback_actor_counts"].get(fallback_actor_key, 0) or 0
            ) + sample_increment
        if workload_type == "plsql":
            entry["source_workload_type"] = "plsql"
        if row.get("plsql_profile_status") and not entry.get("plsql_profile_status"):
            entry["plsql_profile_status"] = row.get("plsql_profile_status")
        if row.get("plsql_profile_summary") and not entry.get("plsql_profile_summary"):
            entry["plsql_profile_summary"] = row.get("plsql_profile_summary")
        if row.get("plsql_profile_mapping_summary") and not entry.get("plsql_profile_mapping_summary"):
            entry["plsql_profile_mapping_summary"] = row.get("plsql_profile_mapping_summary")
        if row.get("plsql_profile_diagnosis_summary") and not entry.get("plsql_profile_diagnosis_summary"):
            entry["plsql_profile_diagnosis_summary"] = row.get("plsql_profile_diagnosis_summary")
        if row.get("plsql_profile_diagnoses") and not entry.get("plsql_profile_diagnoses"):
            entry["plsql_profile_diagnoses"] = row.get("plsql_profile_diagnoses") or []
        request_id = _safe_int(row.get("source_ob_request_id"))
        current_request_id = _safe_int(entry.get("source_ob_request_id"))
        if request_id is not None and (current_request_id is None or request_id >= current_request_id):
            entry["source_ob_request_id"] = row.get("source_ob_request_id")
            entry["source_ob_trace_id"] = row.get("source_ob_trace_id")
            entry["source_ob_plan_type_raw"] = row.get("source_ob_plan_type_raw")
            entry["captured_at"] = row.get("captured_at")
            entry["source_tenant_name"] = row.get("source_tenant_name")
            entry["source_db_name"] = row.get("source_db_name")
            entry["source_user_name"] = row.get("source_user_name")
            entry["source_user_client_ip"] = row.get("source_user_client_ip")
            entry["source_client_ip"] = row.get("source_client_ip")
            entry["source_ret_code"] = row.get("source_ret_code")
            entry["source_last_seen_at"] = row.get("captured_at")
        first_seen = str(entry.get("source_first_seen_at") or "")
        row_seen = str(row.get("captured_at") or "")
        if row_seen and (not first_seen or row_seen < first_seen):
            entry["source_first_seen_at"] = row_seen
        row_source = str(row.get("source_sql_text_source") or row.get("source_sql_text_status") or "").strip() or "missing"
        entry_source = str(entry.get("source_sql_text_source") or "").strip() or "missing"
        if source_priority.get(row_source, 0) >= source_priority.get(entry_source, 0):
            entry["source_sql_text_source"] = row_source

    aggregated_rows = []
    for entry in grouped.values():
        sample_count = max(1, int(entry.get("source_sample_count") or 1))
        actor_counts = entry.get("source_actor_counts") or {}
        fallback_actor_counts = entry.get("source_fallback_actor_counts") or {}
        sorted_actor_counts = sorted(
            actor_counts.items(),
            key=lambda item: (-int(item[1] or 0), item[0]),
        )
        sorted_fallback_actor_counts = sorted(
            fallback_actor_counts.items(),
            key=lambda item: (-int(item[1] or 0), item[0]),
        )
        direct_sample_count = sum(int(count or 0) for _, count in sorted_actor_counts)
        fallback_sample_count = sum(int(count or 0) for _, count in sorted_fallback_actor_counts)
        if sorted_actor_counts:
            primary_actor = sorted_actor_counts[0][0]
            primary_actor_count = int(sorted_actor_counts[0][1] or 0)
            attribution_quality = "mixed" if sorted_fallback_actor_counts else "direct"
            effective_actor_summaries = sorted_actor_counts
        elif sorted_fallback_actor_counts:
            primary_actor = sorted_fallback_actor_counts[0][0]
            primary_actor_count = int(sorted_fallback_actor_counts[0][1] or 0)
            attribution_quality = "fallback"
            effective_actor_summaries = sorted_fallback_actor_counts
        else:
            primary_actor = "unattributed"
            primary_actor_count = 0
            attribution_quality = "unattributed"
            effective_actor_summaries = []
        aggregated = {
            "sql_id": entry.get("sql_id"),
            "sql_text": entry.get("sql_text"),
            "sql_text_normalized": entry.get("sql_text_normalized"),
            "schema": entry.get("schema"),
            "source": entry.get("source"),
            "captured_at": entry.get("captured_at"),
            "baseline_source_mode": SOURCE_DB_MODE_OCEANBASE,
            "ob_status": "ok",
            "source_sample_count": sample_count,
            "source_total_elapsed_us": entry.get("source_total_elapsed_us"),
            "ob_elapsed_us": float(entry.get("source_total_elapsed_us") or 0.0) / sample_count,
            "ob_queue_time_us": float(entry.get("source_total_queue_time_us") or 0.0) / sample_count,
            "ob_get_plan_time_us": float(entry.get("source_total_get_plan_time_us") or 0.0) / sample_count,
            "ob_execute_time_us": float(entry.get("source_total_execute_time_us") or 0.0) / sample_count,
            "ob_net_time_us": float(entry.get("source_total_net_time_us") or 0.0) / sample_count,
            "ob_net_wait_time_us": float(entry.get("source_total_net_wait_time_us") or 0.0) / sample_count,
            "ob_plan_type_raw": entry.get("source_ob_plan_type_raw"),
            "ob_is_hit_plan": "1" if int(entry.get("source_plan_miss_count") or 0) == 0 else "0",
            "ob_is_executor_rpc": "1" if int(entry.get("source_rpc_count") or 0) > 0 else "0",
            "ob_logical_reads": float(entry.get("source_total_logical_reads") or 0.0) / sample_count,
            "ob_physical_reads": float(entry.get("source_total_physical_reads") or 0.0) / sample_count,
            "ob_retry_cnt": float(entry.get("source_total_retry_cnt") or 0.0) / sample_count,
            "ob_memstore_read_rows": float(entry.get("source_total_memstore_read_rows") or 0.0) / sample_count,
            "ob_ssstore_read_rows": float(entry.get("source_total_ssstore_read_rows") or 0.0) / sample_count,
            "ob_bloom_filter_filtered": float(entry.get("source_total_bloom_filter_filtered") or 0.0) / sample_count,
            "source_ob_trace_id": entry.get("source_ob_trace_id"),
            "source_ob_request_id": entry.get("source_ob_request_id"),
            "source_sql_text_source": entry.get("source_sql_text_source"),
            "source_workload_type": entry.get("source_workload_type") or classify_source_workload_type(entry.get("sql_text")),
            "source_actor_fields": entry.get("source_actor_fields") or list(DEFAULT_SOURCE_ACTOR_FIELDS),
            "source_actor_count": len(effective_actor_summaries),
            "source_primary_actor": primary_actor,
            "source_primary_actor_count": primary_actor_count,
            "source_attribution_quality": attribution_quality,
            "source_direct_sample_count": direct_sample_count,
            "source_fallback_sample_count": fallback_sample_count,
            "source_direct_actor_count": len(sorted_actor_counts),
            "source_fallback_actor_count": len(sorted_fallback_actor_counts),
            "source_direct_actor_summaries": [
                {"actor": actor, "count": int(count)}
                for actor, count in sorted_actor_counts[:5]
            ],
            "source_fallback_actor_summaries": [
                {"actor": actor, "count": int(count)}
                for actor, count in sorted_fallback_actor_counts[:5]
            ],
            "source_actor_summaries": [
                {"actor": actor, "count": int(count)}
                for actor, count in effective_actor_summaries[:5]
            ],
            "source_first_seen_at": entry.get("source_first_seen_at"),
            "source_last_seen_at": entry.get("source_last_seen_at") or entry.get("captured_at"),
            "source_tenant_name": entry.get("source_tenant_name"),
            "source_db_name": entry.get("source_db_name"),
            "source_user_name": entry.get("source_user_name"),
            "source_user_client_ip": entry.get("source_user_client_ip"),
            "source_client_ip": entry.get("source_client_ip"),
            "source_ret_code": entry.get("source_ret_code"),
            "plsql_profile_status": entry.get("plsql_profile_status"),
            "plsql_profile_summary": entry.get("plsql_profile_summary"),
            "plsql_profile_mapping_summary": entry.get("plsql_profile_mapping_summary"),
            "plsql_profile_diagnosis_summary": entry.get("plsql_profile_diagnosis_summary"),
            "plsql_profile_diagnoses": entry.get("plsql_profile_diagnoses") or [],
            "plan_monitor_rows": [],
        }
        aggregated.update(derive_replay_metrics(aggregated))
        aggregated["plan_diff_signals"] = []
        aggregated_rows.append(aggregated)
    return aggregated_rows


def capture_workload(config, args, run_id):
    # type: (AppConfig, argparse.Namespace, str) -> Path
    if config.settings.get("source_db_mode") == SOURCE_DB_MODE_OCEANBASE:
        return capture_workload_from_ob_source(config, args, run_id)
    capture_capabilities = probe_oracle_capabilities(config)
    capture_capabilities["run_id"] = run_id
    if config.oracle_source:
        capture_capabilities["oracle_dsn"] = config.oracle_source.get("dsn")
    replay_capabilities = probe_replay_capabilities(config)
    replay_capabilities["run_id"] = run_id
    write_capability_files(config, run_id, capture_capabilities, replay_capabilities)
    if getattr(args, "sql_file", None):
        return capture_from_sql_file(config, args.sql_file, run_id)
    wcr_path = str(getattr(args, "wcr_path", None) or config.settings.get("wcr_path") or "").strip()
    if capture_capabilities.get("awr"):
        preferred_source = "awr"
    elif capture_capabilities.get("vsql"):
        preferred_source = "vsql"
    elif capture_capabilities.get("unified_audit"):
        preferred_source = "unified_audit"
    elif wcr_path and capture_capabilities.get("wcr"):
        return capture_from_wcr_file(config, wcr_path, run_id)
    else:
        raise ConfigError("Oracle capture is unavailable and --sql-file was not provided")
    connection = _open_oracle_connection(config)
    try:
        if preferred_source == "awr":
            rows = _capture_from_awr(connection, config)
        elif preferred_source == "unified_audit":
            rows = _capture_from_unified_audit(connection, config)
        else:
            rows = _capture_from_vsql(connection, config)
    finally:
        connection.close()
    if not rows:
        raise ConfigError("Oracle capture did not return any SQL statements")
    workload_path = build_artifact_path("workload", run_id, root_dir=config.settings["workloads_dir"])
    append_jsonl(workload_path, rows)
    return workload_path


def stream_capture_workload(config, args, run_id):
    # type: (AppConfig, argparse.Namespace, str) -> Path
    if getattr(args, "sql_file", None):
        raise ConfigError("stream mode does not support --sql-file")
    capture_capabilities = probe_oracle_capabilities(config)
    capture_capabilities["run_id"] = run_id
    capture_capabilities["oracle_dsn"] = config.oracle_source["dsn"]
    replay_capabilities = probe_replay_capabilities(config)
    replay_capabilities["run_id"] = run_id
    write_capability_files(config, run_id, capture_capabilities, replay_capabilities)
    if not capture_capabilities.get("vsql"):
        raise ConfigError("stream mode requires Oracle V$SQL access")
    duration = int(getattr(args, "duration", 0) or config.settings.get("duration", config.settings["interval"]))
    if duration <= 0:
        duration = config.settings["interval"]
    workload_path = build_artifact_path("workload", run_id, root_dir=config.settings["workloads_dir"])
    started = time.time()
    watermark = None  # type: Optional[str]
    connection = _open_oracle_connection(config)
    try:
        while True:
            rows = _capture_from_vsql(connection, config, since_ts=watermark)
            new_rows = []
            for row in rows:
                row["source"] = "stream_vsql"
                new_rows.append(row)
            if new_rows:
                append_jsonl(workload_path, new_rows)
                watermark = max(str(row.get("captured_at") or "") for row in new_rows) or watermark
            if time.time() - started >= duration:
                break
            time.sleep(int(config.settings.get("interval", DEFAULT_INTERVAL)))
    finally:
        connection.close()
    if not workload_path.exists():
        raise ConfigError("stream mode did not capture any workload rows")
    return workload_path


def maybe_refresh_replay_report(config, replay_path, run_id, workload_path, last_refresh_at, force=False):
    # type: (AppConfig, Union[str, Path], str, Union[str, Path], float, bool) -> float
    refresh_interval = int(config.settings.get("rolling_report_interval", DEFAULT_ROLLING_REPORT_INTERVAL) or 0)
    if refresh_interval <= 0:
        return float(last_refresh_at or 0.0)
    now = time.time()
    if not force and now - float(last_refresh_at or 0.0) < refresh_interval:
        return float(last_refresh_at or 0.0)
    if not Path(replay_path).exists():
        return float(last_refresh_at or 0.0)
    rolling_config = clone_app_config(config, settings_updates={"_rolling_stream_report": True})
    generate_report_from_replay(rolling_config, replay_path, run_id, workload_path)
    LOG.info("Rolling Oracle->OB report refreshed: run_id=%s", run_id)
    return now


def run_stream_monitor_pipeline(config, args, run_id):
    # type: (AppConfig, argparse.Namespace, str) -> Dict[str, Path]
    if getattr(args, "sql_file", None):
        raise ConfigError("stream mode does not support --sql-file")
    config.settings["_current_run_id"] = run_id
    capture_capabilities = probe_oracle_capabilities(config)
    capture_capabilities["run_id"] = run_id
    capture_capabilities["oracle_dsn"] = config.oracle_source["dsn"]
    replay_capabilities = probe_replay_capabilities(config)
    replay_capabilities["run_id"] = run_id
    write_capability_files(config, run_id, capture_capabilities, replay_capabilities)
    if not capture_capabilities.get("vsql"):
        raise ConfigError("stream mode requires Oracle V$SQL access")
    duration = int(getattr(args, "duration", 0) or config.settings.get("duration", config.settings["interval"]))
    if duration <= 0:
        duration = config.settings["interval"]
    workload_path = build_artifact_path("workload", run_id, root_dir=config.settings["workloads_dir"])
    replay_path = build_artifact_path("replay", run_id, root_dir=config.settings["workloads_dir"])
    started = time.time()
    watermark = None  # type: Optional[str]
    last_report_refresh_at = 0.0
    seen_fingerprints = set()  # type: Set[str]
    connection = _open_oracle_connection(config)
    collector = None  # type: Optional[SQLAuditCollector]
    backend = ObclientReplayBackend()
    if replay_capabilities.get("sql_audit"):
        audit_dump_path = build_artifact_path("audit_dump", run_id, root_dir=config.settings["workloads_dir"])
        collector = SQLAuditCollector(config, audit_dump_path)
        collector.start()
    try:
        while True:
            rows = _capture_from_vsql(connection, config, since_ts=watermark)
            if rows:
                watermark = max(str(row.get("captured_at") or "") for row in rows) or watermark
            new_rows = []
            for row in rows:
                row["source"] = "stream_vsql"
                row["workload_identity"] = build_workload_identity(row)
                row["workload_event_id"] = build_workload_event_id(row)
                if row["workload_identity"] in seen_fingerprints:
                    continue
                seen_fingerprints.add(row["workload_identity"])
                new_rows.append(row)
            if new_rows:
                append_jsonl(workload_path, new_rows)
                replay_rows = []
                for workload_row in new_rows:
                    replay_row = replay_statement(
                        config, workload_row, audit_collector=collector, backend=backend
                    )
                    replay_row["workload_identity"] = workload_row.get("workload_identity")
                    replay_row["workload_event_id"] = workload_row.get("workload_event_id")
                    replay_rows.append(replay_row)
                append_jsonl(replay_path, replay_rows)
                last_report_refresh_at = maybe_refresh_replay_report(
                    config, replay_path, run_id, workload_path, last_report_refresh_at
                )
            if time.time() - started >= duration:
                break
            time.sleep(int(config.settings.get("interval", DEFAULT_INTERVAL)))
    finally:
        if collector is not None:
            collector.stop()
        connection.close()
    if not workload_path.exists():
        raise ConfigError("stream mode did not capture any workload rows")
    if not replay_path.exists():
        raise ConfigError("stream mode did not produce any replay rows")
    last_report_refresh_at = maybe_refresh_replay_report(
        config, replay_path, run_id, workload_path, last_report_refresh_at, force=True
    )
    report_paths = generate_report_from_replay(config, replay_path, run_id, workload_path)
    LOG.info(
        "Rolling stream monitor complete: workload=%s replay=%s summary=%s",
        workload_path,
        replay_path,
        report_paths["summary"],
    )
    return {"workload": workload_path, "replay": replay_path, "summary": report_paths["summary"]}


def _query_recent_audit_row(config, rendered_sql):
    # type: (AppConfig, str) -> Dict[str, Any]
    snippet = normalize_sql_text(rendered_sql)[:80].replace("'", "''")
    if not snippet:
        return {}
    query = """
        SELECT * FROM (
          SELECT
            REQUEST_ID,
            ELAPSED_TIME,
            QUEUE_TIME,
            GET_PLAN_TIME,
            EXECUTE_TIME,
            NET_TIME,
            NET_WAIT_TIME,
            PLAN_TYPE,
            IS_HIT_PLAN,
            IS_EXECUTOR_RPC,
            LOGICAL_READ_COUNT,
            PHYSICAL_READ_COUNT
          FROM GV$OB_SQL_AUDIT
          WHERE UPPER(REPLACE(REPLACE(REPLACE(SQL_TEXT, CHR(10), ' '), CHR(13), ' '), CHR(9), ' ')) LIKE '%{snippet}%'
          ORDER BY REQUEST_ID DESC
        ) WHERE ROWNUM = 1
    """.format(snippet=snippet)
    ok, stdout, stderr = obclient_run_sql(
        config.oceanbase_target,
        query,
        timeout=config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT),
        session_query_timeout_us=config.settings.get(
            "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
        ),
    )
    if not ok or not stdout:
        return {}
    fields = stdout.splitlines()[0].split("\t")
    if len(fields) < 12:
        return {}
    return {
        "request_id": fields[0],
        "ob_elapsed_us": _safe_float(fields[1]),
        "ob_queue_time_us": _safe_float(fields[2]),
        "ob_get_plan_time_us": _safe_float(fields[3]),
        "ob_execute_time_us": _safe_float(fields[4]),
        "ob_net_time_us": _safe_float(fields[5]),
        "ob_net_wait_time_us": _safe_float(fields[6]),
        "ob_plan_type_raw": fields[7],
        "ob_is_hit_plan": fields[8],
        "ob_is_executor_rpc": fields[9],
        "ob_logical_reads": _safe_float(fields[10]),
        "ob_physical_reads": _safe_float(fields[11]),
        "sql_text": rendered_sql,
    }


def _query_recent_audit_row_by_sql_id(config, sql_id, rendered_sql):
    # type: (AppConfig, str, str) -> Dict[str, Any]
    if not sql_id:
        return {}
    query = """
        SELECT * FROM (
          SELECT
            REQUEST_ID,
            ELAPSED_TIME,
            QUEUE_TIME,
            GET_PLAN_TIME,
            EXECUTE_TIME,
            NET_TIME,
            NET_WAIT_TIME,
            PLAN_TYPE,
            IS_HIT_PLAN,
            IS_EXECUTOR_RPC,
            LOGICAL_READ_COUNT,
            PHYSICAL_READ_COUNT,
            RETRY_CNT,
            MEMSTORE_READ_ROW_COUNT,
            SSSTORE_READ_ROW_COUNT,
            BLOOM_FILTER_FILTERED_COUNT
          FROM GV$OB_SQL_AUDIT
          WHERE SQL_ID = '{sql_id}'
          ORDER BY REQUEST_ID DESC
        ) WHERE ROWNUM = 1
    """.format(sql_id=str(sql_id).replace("'", "''"))
    ok, stdout, _ = obclient_run_sql(
        config.oceanbase_target,
        query,
        timeout=config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT),
        session_query_timeout_us=config.settings.get(
            "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
        ),
    )
    if not ok or not stdout:
        return {}
    fields = stdout.splitlines()[0].split("\t")
    if len(fields) < 16:
        return {}
    return {
        "request_id": fields[0],
        "ob_elapsed_us": _safe_float(fields[1]),
        "ob_queue_time_us": _safe_float(fields[2]),
        "ob_get_plan_time_us": _safe_float(fields[3]),
        "ob_execute_time_us": _safe_float(fields[4]),
        "ob_net_time_us": _safe_float(fields[5]),
        "ob_net_wait_time_us": _safe_float(fields[6]),
        "ob_plan_type_raw": fields[7],
        "ob_is_hit_plan": fields[8],
        "ob_is_executor_rpc": fields[9],
        "ob_logical_reads": _safe_float(fields[10]),
        "ob_physical_reads": _safe_float(fields[11]),
        "ob_retry_cnt": _safe_float(fields[12]),
        "ob_memstore_read_rows": _safe_float(fields[13]),
        "ob_ssstore_read_rows": _safe_float(fields[14]),
        "ob_bloom_filter_filtered": _safe_float(fields[15]),
        "sql_text": rendered_sql,
    }


def _safe_float(value):
    # type: (Any) -> Optional[float]
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value):
    # type: (Any) -> Optional[int]
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _is_truthy_flag(value):
    # type: (Any) -> bool
    return str(value or "").strip() in ("1", "True", "true", "YES", "yes")


def _escape_sql_string(value):
    # type: (str) -> str
    return str(value or "").replace("'", "''")


def _parse_version_string(version_text):
    # type: (Any) -> Optional[str]
    for raw_line in str(version_text or "").splitlines():
        line = raw_line.strip()
        if not line or line.upper() == "OB_VERSION()":
            continue
        if "." in line and line.replace(".", "").replace("-", "").isdigit():
            return line.split("-", 1)[0]
    return None


def get_oceanbase_version(config):
    # type: (AppConfig) -> Optional[str]
    cached = config.settings.get("_ob_version")
    if cached is not None:
        return str(cached or "").strip() or None
    ok, stdout, _ = obclient_run_sql(
        config.oceanbase_target,
        "SELECT OB_VERSION() FROM DUAL",
        timeout=config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT),
        session_query_timeout_us=config.settings.get(
            "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
        ),
    )
    version = _parse_version_string(stdout if ok else "")
    config.settings["_ob_version"] = version or ""
    return version


def _decode_source_text_fragment(value):
    # type: (Any) -> str
    return (
        str(value or "")
        .replace(SOURCE_TEXT_LF_SENTINEL, "\n")
        .replace(SOURCE_TEXT_CR_SENTINEL, "\r")
    )


def _normalize_source_line_text(value):
    # type: (Any) -> str
    return str(value or "").replace("\r", "")


def _split_source_blob_text(value):
    # type: (Any) -> List[str]
    normalized = _decode_source_text_fragment(value).replace("\r\n", "\n")
    parts = normalized.split("\n")
    if parts and parts[-1] == "":
        parts = parts[:-1]
    return [_normalize_source_line_text(part) for part in parts]


def _query_plsql_source_rows(config, source_view, owner, unit_name, unit_type):
    # type: (AppConfig, str, str, str, str) -> Dict[str, Any]
    query = """
        SELECT
          LINE,
          REPLACE(REPLACE(TEXT, CHR(13), CHR(30)), CHR(10), CHR(31))
        FROM {source_view}
        WHERE OWNER = '{owner}'
          AND NAME = '{unit_name}'
          AND TYPE = '{unit_type}'
        ORDER BY LINE
    """.format(
        source_view=source_view,
        owner=_escape_sql_string(owner),
        unit_name=_escape_sql_string(unit_name),
        unit_type=_escape_sql_string(unit_type),
    )
    ok, stdout, stderr = obclient_run_sql(
        config.oceanbase_target,
        query,
        timeout=config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT),
        session_query_timeout_us=config.settings.get(
            "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
        ),
    )
    if not ok:
        return {"rows": [], "error": (stderr or stdout or "").strip(), "source_view": source_view}
    rows = []
    for raw_line in (stdout or "").splitlines():
        fields = raw_line.split("\t", 1)
        if not fields:
            continue
        line_value = _safe_int(fields[0])
        if line_value is None:
            continue
        rows.append(
            {
                "line": line_value,
                "text": _decode_source_text_fragment(fields[1] if len(fields) > 1 else ""),
            }
        )
    return {"rows": rows, "error": "", "source_view": source_view}


def _build_plsql_source_line_map(source_rows):
    # type: (List[Dict[str, Any]]) -> Tuple[Dict[int, str], str]
    if not source_rows:
        return {}, "unavailable"
    if len(source_rows) == 1 and "\n" in str(source_rows[0].get("text") or ""):
        line_map = {}
        base_line = max(1, int(source_rows[0].get("line") or 1))
        for offset, line_text in enumerate(_split_source_blob_text(source_rows[0].get("text"))):
            line_map[base_line + offset] = line_text
        return line_map, "single_row_clob"
    if any("\n" in str(row.get("text") or "") for row in source_rows):
        line_map = {}
        for row in source_rows:
            base_line = max(1, int(row.get("line") or 1))
            split_lines = _split_source_blob_text(row.get("text"))
            if not split_lines:
                line_map[base_line] = ""
                continue
            for offset, line_text in enumerate(split_lines):
                line_map[base_line + offset] = line_text
        return line_map, "embedded_newlines"
    return {
        int(row.get("line") or 0): _normalize_source_line_text(row.get("text"))
        for row in source_rows
        if _safe_int(row.get("line")) is not None
    }, "line_rows"


def _source_mapping_strategy_for_layout(source_view, source_layout):
    # type: (str, str) -> str
    source_key = str(source_view or "").strip().lower()
    if source_layout == "line_rows":
        return "%s_line_rows" % source_key
    if source_layout in ("single_row_clob", "embedded_newlines"):
        return "%s_blob_split" % source_key
    return "none"


def _source_mapping_confidence_for_layout(source_layout):
    # type: (str) -> str
    if source_layout == "line_rows":
        return "high"
    if source_layout in ("single_row_clob", "embedded_newlines"):
        return "medium"
    return "none"


def _load_plsql_source_lines(config, owner, unit_name, unit_type):
    # type: (AppConfig, str, str, str) -> Dict[str, Any]
    cache = config.settings.setdefault("_plsql_source_cache", {})
    cache_key = "%s|%s|%s" % (owner, unit_name, unit_type)
    if cache_key in cache:
        return dict(cache[cache_key])
    ob_version = get_oceanbase_version(config)
    errors = []
    for source_view in ("DBA_SOURCE", "ALL_SOURCE"):
        query_result = _query_plsql_source_rows(config, source_view, owner, unit_name, unit_type)
        if query_result.get("error"):
            errors.append("%s:%s" % (source_view, query_result.get("error")))
        source_rows = query_result.get("rows") or []
        if not source_rows:
            continue
        line_map, source_layout = _build_plsql_source_line_map(source_rows)
        source_info = {
            "owner": owner,
            "unit_name": unit_name,
            "unit_type": unit_type,
            "lines": line_map,
            "source_view": source_view,
            "source_layout": source_layout,
            "source_mapping_strategy": _source_mapping_strategy_for_layout(source_view, source_layout),
            "source_mapping_confidence": _source_mapping_confidence_for_layout(source_layout),
            "ob_version": ob_version or "unknown",
            "source_errors": errors,
        }
        cache[cache_key] = dict(source_info)
        return source_info
    source_info = {
        "owner": owner,
        "unit_name": unit_name,
        "unit_type": unit_type,
        "lines": {},
        "source_view": "",
        "source_layout": "unavailable",
        "source_mapping_strategy": "none",
        "source_mapping_confidence": "none",
        "ob_version": ob_version or "unknown",
        "source_errors": errors,
    }
    cache[cache_key] = dict(source_info)
    return source_info


def _probe_plsql_profiler_capability(config):
    # type: (AppConfig) -> Dict[str, Any]
    query = """
        SELECT COUNT(*)
        FROM ALL_OBJECTS
        WHERE OBJECT_NAME = 'DBMS_PROFILER'
          AND OBJECT_TYPE = 'PACKAGE'
    """
    ok, stdout, stderr = obclient_run_sql(
        config.oceanbase_target,
        query,
        timeout=config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT),
        session_query_timeout_us=config.settings.get(
            "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
        ),
    )
    if not ok:
        return {
            "available": False,
            "status": "unavailable",
            "error": stderr or stdout,
            "ob_version": get_oceanbase_version(config) or "unknown",
        }
    count_value = _safe_int((stdout or "").splitlines()[0].split("\t")[0] if stdout.strip() else None) or 0
    if count_value > 0:
        return {
            "available": True,
            "status": "ready",
            "error": "",
            "ob_version": get_oceanbase_version(config) or "unknown",
        }
    return {
        "available": False,
        "status": "unavailable",
        "error": "DBMS_PROFILER package not found",
        "ob_version": get_oceanbase_version(config) or "unknown",
    }


def _probe_ocp_capability(config):
    # type: (AppConfig) -> Dict[str, Any]
    base_url = str(config.settings.get("ocp_base_url") or "").strip()
    auth_env = str(config.settings.get("ocp_authorization_env") or "").strip()
    username = str(config.settings.get("ocp_username") or "").strip()
    password_env = str(config.settings.get("ocp_password_env") or "").strip()
    password = str(config.settings.get("ocp_password") or "").strip()
    cluster_id = str(config.settings.get("ocp_cluster_id") or "").strip()
    tenant_id = str(config.settings.get("ocp_tenant_id") or "").strip()
    cluster_name = str(config.settings.get("ocp_cluster_name") or "").strip()
    tenant_name = str(config.settings.get("ocp_tenant_name") or "").strip()
    if base_url or auth_env or username or password_env or password or cluster_id or tenant_id or cluster_name or tenant_name:
        missing = []
        if not base_url:
            missing.append("ocp_base_url")
        has_auth = False
        if auth_env and os.environ.get(auth_env):
            has_auth = True
        elif username and ((password_env and os.environ.get(password_env)) or password):
            has_auth = True
        if not has_auth:
            missing.append("ocp auth")
        has_target = bool((cluster_id and tenant_id) or (cluster_name and tenant_name))
        if not has_target:
            missing.append("ocp cluster/tenant")
        if missing:
            return {
                "available": False,
                "status": "misconfigured",
                "mode": "native",
                "error": "missing native OCP settings: %s" % ", ".join(missing),
            }
        return {
            "available": True,
            "status": "ready",
            "mode": "native",
            "error": "",
            "base_url": base_url,
            "cluster_id": cluster_id or cluster_name,
            "tenant_id": tenant_id or tenant_name,
        }
    ash_template = str(config.settings.get("ocp_ash_url_template") or "").strip()
    qpm_template = str(config.settings.get("ocp_qpm_url_template") or "").strip()
    if not ash_template and not qpm_template:
        return {"available": False, "status": "unconfigured", "error": ""}
    token_env = str(config.settings.get("ocp_auth_token_env") or "").strip()
    if token_env and not os.environ.get(token_env):
        return {
            "available": False,
            "status": "misconfigured",
            "error": "missing auth token env: %s" % token_env,
        }
    return {
        "available": True,
        "status": "ready",
        "mode": "template",
        "error": "",
        "has_ash": bool(ash_template),
        "has_qpm": bool(qpm_template),
    }


def _probe_obdiag_capability(config):
    # type: (AppConfig) -> Dict[str, Any]
    executable = str(config.settings.get("obdiag_executable") or "").strip()
    if not executable:
        return {"available": False, "status": "unconfigured", "error": ""}
    if not Path(executable).exists():
        return {"available": False, "status": "unavailable", "error": "obdiag executable not found"}
    return {"available": True, "status": "ready", "error": "", "executable": executable}


def has_external_diagnostics_config(config):
    # type: (AppConfig) -> bool
    return bool(
        str(config.settings.get("ocp_base_url") or "").strip()
        or str(config.settings.get("ocp_authorization_env") or "").strip()
        or str(config.settings.get("ocp_cluster_id") or "").strip()
        or str(config.settings.get("ocp_tenant_id") or "").strip()
        or str(config.settings.get("ocp_ash_url_template") or "").strip()
        or str(config.settings.get("ocp_qpm_url_template") or "").strip()
        or str(config.settings.get("obdiag_executable") or "").strip()
    )


def ensure_plsql_profiler_initialized(config):
    # type: (AppConfig) -> Tuple[bool, str]
    cached = config.settings.get("_plsql_profiler_init_status")
    if isinstance(cached, dict):
        return bool(cached.get("ok")), str(cached.get("reason") or "")
    init_sql = "BEGIN DBMS_PROFILER.OB_INIT_OBJECTS(FALSE); END;"
    ok, stdout, stderr = obclient_run_sql(
        config.oceanbase_target,
        init_sql,
        timeout=config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT),
        session_query_timeout_us=config.settings.get(
            "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
        ),
    )
    combined = " ".join(part for part in (stdout, stderr) if part).lower()
    if ok or "already exists" in combined:
        config.settings["_plsql_profiler_init_status"] = {"ok": True, "reason": ""}
        return True, ""
    reason = (stderr or stdout or "profiler_init_failed").strip()
    config.settings["_plsql_profiler_init_status"] = {"ok": False, "reason": reason}
    return False, reason


def _build_plsql_profiler_payload(rendered_sql, profiler_comment):
    # type: (str, str) -> str
    statement_sql = str(rendered_sql or "").strip().rstrip(";") + ";"
    indented_statement = "\n".join("  " + line for line in statement_sql.splitlines())
    return "\n".join(
        [
            "BEGIN",
            "  DBMS_PROFILER.START_PROFILER('%s');" % _escape_sql_string(profiler_comment),
            indented_statement,
            "  DBMS_PROFILER.STOP_PROFILER();",
            "END;",
        ]
    )


def _should_retry_profiler_sequentially(error_text):
    # type: (Any) -> bool
    normalized = str(error_text or "").upper()
    return "ORA-00900" in normalized and ("NEAR 'BEGIN'" in normalized or "NEAR 'CALL'" in normalized)


def _build_plsql_profiler_single_block_payload(rendered_sql, profiler_comment):
    # type: (str, str) -> str
    normalized_sql = render_sql_for_replay(rendered_sql, {})[0] or str(rendered_sql or "").strip()
    statement_sql = str(normalized_sql or "").strip().rstrip(";") + ";"
    indented_statement = "\n".join("  " + line for line in statement_sql.splitlines())
    return "\n".join(
        [
            "BEGIN",
            "  DBMS_PROFILER.START_PROFILER('%s');" % _escape_sql_string(profiler_comment),
            indented_statement,
            "  DBMS_PROFILER.STOP_PROFILER();",
            "END;",
        ]
    )


def _lookup_plsql_profile_runid(config, profiler_comment):
    # type: (AppConfig, str) -> Optional[int]
    query = """
        SELECT * FROM (
          SELECT RUNID
          FROM PLSQL_PROFILER_RUNS
          WHERE RUN_COMMENT = '{comment}'
          ORDER BY RUNID DESC
        ) WHERE ROWNUM = 1
    """.format(comment=_escape_sql_string(profiler_comment))
    ok, stdout, _ = obclient_run_sql(
        config.oceanbase_target,
        query,
        timeout=config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT),
        session_query_timeout_us=config.settings.get(
            "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
        ),
    )
    if not ok or not stdout.strip():
        return None
    first_line = stdout.splitlines()[0].split("\t")[0]
    return _safe_int(first_line)


def _fetch_plsql_source_context(config, owner, unit_name, unit_type, line_no, context_size, source_info=None):
    # type: (AppConfig, str, str, str, int, int, Optional[Dict[str, Any]]) -> List[Dict[str, Any]]
    source_payload = dict(source_info or _load_plsql_source_lines(config, owner, unit_name, unit_type))
    line_map = source_payload.get("lines") or {}
    low_line = max(1, int(line_no) - max(0, int(context_size or 0)))
    high_line = int(line_no) + max(0, int(context_size or 0))
    context_rows = []
    for line_value in range(low_line, high_line + 1):
        if line_value not in line_map:
            continue
        context_rows.append({"line": line_value, "text": line_map.get(line_value) or ""})
    return context_rows


def _fetch_plsql_profile_top_lines(config, runid, top_n, context_size):
    # type: (AppConfig, int, int, int) -> List[Dict[str, Any]]
    query = """
        SELECT * FROM (
          SELECT
            u.UNIT_OWNER,
            u.UNIT_NAME,
            u.UNIT_TYPE,
            d.LINE#,
            d.TOTAL_OCCUR,
            d.TOTAL_TIME
          FROM PLSQL_PROFILER_UNITS u
          JOIN PLSQL_PROFILER_DATA d
            ON d.RUNID = u.RUNID
           AND d.UNIT_NUMBER = u.UNIT_NUMBER
          WHERE u.RUNID = {runid}
          ORDER BY d.TOTAL_TIME DESC, d.LINE#
        ) WHERE ROWNUM <= {top_n}
    """.format(runid=int(runid), top_n=max(1, int(top_n or 1)))
    ok, stdout, _ = obclient_run_sql(
        config.oceanbase_target,
        query,
        timeout=config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT),
        session_query_timeout_us=config.settings.get(
            "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
        ),
    )
    if not ok or not stdout.strip():
        return []
    hot_lines = []
    for raw_line in stdout.splitlines():
        fields = raw_line.split("\t")
        if len(fields) < 6:
            continue
        line_no = _safe_int(fields[3])
        if line_no is None:
            continue
        source_info = _load_plsql_source_lines(config, fields[0], fields[1], fields[2])
        source_map = source_info.get("lines") or {}
        source_text = source_map.get(line_no) or ""
        mapping_confidence = source_info.get("source_mapping_confidence") or "none"
        if not source_text:
            mapping_confidence = "low" if source_map else "none"
        row = {
            "owner": fields[0],
            "unit_name": fields[1],
            "unit_type": fields[2],
            "line": line_no,
            "total_occur": _safe_int(fields[4]),
            "total_time_us": _safe_float(fields[5]),
            "source_text": source_text,
            "source_mapping_strategy": source_info.get("source_mapping_strategy") or "none",
            "source_mapping_confidence": mapping_confidence,
            "source_view": source_info.get("source_view") or "",
            "source_layout": source_info.get("source_layout") or "unavailable",
            "ob_version": source_info.get("ob_version") or "unknown",
            "source_line_hit": bool(source_text),
        }
        if int(context_size or 0) > 0:
            row["context_lines"] = _fetch_plsql_source_context(
                config,
                fields[0],
                fields[1],
                fields[2],
                line_no,
                int(context_size or 0),
                source_info=source_info,
            )
            if str(source_text or "").strip().upper() == "NULL" and row["context_lines"]:
                matched_context = [
                    context_row.get("text")
                    for context_row in row["context_lines"]
                    if int(context_row.get("line") or 0) == line_no and str(context_row.get("text") or "").strip()
                ]
                if matched_context:
                    row["source_text"] = matched_context[0]
                else:
                    row["source_text"] = str(row["context_lines"][0].get("text") or "")
        hot_lines.append(row)
    return hot_lines


def _fetch_plsql_profile_unit_summary(config, runid):
    # type: (AppConfig, int) -> List[Dict[str, Any]]
    query = """
        SELECT
          u.UNIT_OWNER,
          u.UNIT_NAME,
          u.UNIT_TYPE,
          SUM(d.TOTAL_TIME),
          SUM(d.TOTAL_OCCUR)
        FROM PLSQL_PROFILER_UNITS u
        JOIN PLSQL_PROFILER_DATA d
          ON d.RUNID = u.RUNID
         AND d.UNIT_NUMBER = u.UNIT_NUMBER
        WHERE u.RUNID = {runid}
        GROUP BY u.UNIT_OWNER, u.UNIT_NAME, u.UNIT_TYPE
        ORDER BY SUM(d.TOTAL_TIME) DESC, SUM(d.TOTAL_OCCUR) DESC
    """.format(runid=int(runid))
    ok, stdout, _ = obclient_run_sql(
        config.oceanbase_target,
        query,
        timeout=config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT),
        session_query_timeout_us=config.settings.get(
            "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
        ),
    )
    if not ok or not stdout.strip():
        return []
    rows = []
    for raw_line in stdout.splitlines():
        fields = raw_line.split("\t")
        if len(fields) < 5:
            continue
        rows.append(
            {
                "owner": fields[0],
                "unit_name": fields[1],
                "unit_type": fields[2],
                "total_time_us": _safe_float(fields[3]),
                "total_occur": _safe_float(fields[4]),
            }
        )
    total_time_us = sum(float(item.get("total_time_us") or 0.0) for item in rows) or 1.0
    for item in rows:
        item["profile_time_ratio"] = float(item.get("total_time_us") or 0.0) / total_time_us
    return rows


def collect_plsql_profile(config, workload_row, rendered_sql, timeout_seconds):
    # type: (AppConfig, Dict[str, Any], str, int) -> Dict[str, Any]
    init_ok, init_reason = ensure_plsql_profiler_initialized(config)
    if not init_ok:
        return {
            "status": "skipped",
            "error": init_reason,
            "runid": None,
            "top_lines": [],
            "artifact_path": "",
        }
    run_group = str(config.settings.get("_current_run_id") or generate_run_id())
    profiler_comment = "perf_comparator:%s:%s:%s" % (
        run_group,
        workload_row.get("sql_id") or compute_sql_id(rendered_sql),
        int(time.time()),
    )
    ok, stdout, stderr = obclient_run_sql(
        config.oceanbase_target,
        _build_plsql_profiler_payload(rendered_sql, profiler_comment),
        timeout=timeout_seconds,
        session_query_timeout_us=config.settings.get(
            "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
        ),
    )
    if not ok and _should_retry_profiler_sequentially(stderr or stdout):
        ok, stdout, stderr = obclient_run_sql(
            config.oceanbase_target,
            _build_plsql_profiler_single_block_payload(rendered_sql, profiler_comment),
            timeout=timeout_seconds,
            session_query_timeout_us=config.settings.get(
                "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
            ),
        )
    if not ok:
        return {
            "status": "error",
            "error": stderr or stdout,
            "runid": None,
            "top_lines": [],
            "artifact_path": "",
        }
    runid = _lookup_plsql_profile_runid(config, profiler_comment)
    if runid is None:
        return {
            "status": "skipped",
            "error": "profiler_run_not_found",
            "runid": None,
            "top_lines": [],
            "artifact_path": "",
        }
    top_lines = _fetch_plsql_profile_top_lines(
        config,
        runid,
        int(config.settings.get("plsql_profile_top_n", DEFAULT_PLSQL_PROFILE_TOP_N)),
        int(config.settings.get("plsql_profile_source_context", DEFAULT_PLSQL_PROFILE_SOURCE_CONTEXT)),
    )
    unit_summary = _fetch_plsql_profile_unit_summary(config, runid)
    analysis = analyze_plsql_profile_evidence(top_lines, unit_summary)
    artifact_path = build_artifact_path(
        "plsql_profile",
        run_group,
        root_dir=config.settings["workloads_dir"],
    )
    append_jsonl(
        artifact_path,
        {
            "sql_id": workload_row.get("sql_id"),
            "sql_text": workload_row.get("sql_text"),
            "rendered_sql": rendered_sql,
            "runid": runid,
            "profiler_comment": profiler_comment,
            "top_lines": top_lines,
            "unit_summary": analysis.get("unit_summary") or [],
            "hot_blocks": analysis.get("hot_blocks") or [],
            "diagnoses": analysis.get("diagnoses") or [],
            "diagnosis_summary": analysis.get("diagnosis_summary") or "",
            "source_mapping_summary": summarize_plsql_profile_mapping(
                {"plsql_profile_status": "ok", "plsql_profile_top_lines": top_lines}
            ),
            "captured_at": utc_now_iso(),
        },
    )
    return {
        "status": "ok",
        "error": "",
        "runid": runid,
        "top_lines": top_lines,
        "unit_summary": analysis.get("unit_summary") or [],
        "hot_blocks": analysis.get("hot_blocks") or [],
        "diagnoses": analysis.get("diagnoses") or [],
        "diagnosis_summary": analysis.get("diagnosis_summary") or "",
        "artifact_path": str(artifact_path),
        "mapping_summary": summarize_plsql_profile_mapping(
            {"plsql_profile_status": "ok", "plsql_profile_top_lines": top_lines}
        ),
    }


def replay_statement(config, workload_row, audit_collector=None, backend=None):
    # type: (AppConfig, Dict[str, Any], Optional[SQLAuditCollector], Optional[ReplayBackend]) -> Dict[str, Any]
    backend = backend or ObclientReplayBackend()
    rendered_sql, skip_reason = render_sql_for_replay(
        workload_row.get("sql_text", ""), workload_row.get("bind_vars")
    )
    if skip_reason:
        merged = dict(workload_row)
        merged.update(
            {
                "sql_text_replayed": None,
                "ob_status": "skip",
                "ob_error_code": skip_reason,
                "ob_wall_time_us": 0.0,
                "ob_elapsed_us": None,
                "ob_net_time_us": 0.0,
                "ob_plan_text": "",
                "ob_plan_error": "",
                "ob_plan_hash": None,
                "replayed_at": utc_now_iso(),
                "recommendations": [],
            }
        )
        merged.update(derive_replay_metrics(merged))
        merged["recommendations"] = build_recommendations(
            merged, slowdown_threshold=float(config.settings.get("slowdown_threshold", DEFAULT_SLOWDOWN_THRESHOLD))
        )
        return merged
    timeout_factor = float(config.settings.get("timeout_factor", DEFAULT_TIMEOUT_FACTOR))
    oracle_avg_elapsed_us = _safe_float(
        workload_row.get("baseline_avg_elapsed_us") or workload_row.get("oracle_avg_elapsed_us")
    )
    timeout_seconds = config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT)
    if oracle_avg_elapsed_us:
        timeout_seconds = max(1, int((oracle_avg_elapsed_us * timeout_factor) / 1000000.0) + 1)
    started_at = utc_now_iso()
    start_perf = time.perf_counter()
    ok, stdout, stderr = backend.execute(config, rendered_sql, timeout_seconds)
    wall_time_us = (time.perf_counter() - start_perf) * 1000000.0
    explain_ok, explain_stdout, explain_stderr = backend.explain(config, rendered_sql)
    plan_rows = parse_explain_plan_text(explain_stdout if explain_ok else "")
    audit_row = {}
    if audit_collector is not None:
        audit_collector.collect_once()
        audit_row = audit_collector.match_for_workload(workload_row, rendered_sql)
    if not audit_row:
        if workload_row.get("baseline_source_mode") == SOURCE_DB_MODE_OCEANBASE:
            audit_row = _query_recent_audit_row_by_sql_id(
                config, str(workload_row.get("sql_id") or ""), rendered_sql
            )
        elif not audit_row:
            audit_row = _query_recent_audit_row(config, rendered_sql)
    replay_row = {
        "sql_id": workload_row.get("sql_id"),
        "sql_text": workload_row.get("sql_text"),
        "sql_text_replayed": rendered_sql,
        "ob_status": "ok" if ok else "error",
        "ob_error_code": stderr if not ok else "",
        "ob_wall_time_us": wall_time_us,
        "ob_elapsed_us": _safe_float(audit_row.get("ob_elapsed_us")) or wall_time_us,
        "ob_queue_time_us": _safe_float(audit_row.get("ob_queue_time_us")),
        "ob_get_plan_time_us": _safe_float(audit_row.get("ob_get_plan_time_us")),
        "ob_execute_time_us": _safe_float(audit_row.get("ob_execute_time_us")),
        "ob_net_time_us": _safe_float(audit_row.get("ob_net_time_us")) or 0.0,
        "ob_net_wait_time_us": _safe_float(audit_row.get("ob_net_wait_time_us")),
        "ob_plan_type_raw": audit_row.get("ob_plan_type_raw"),
        "ob_is_hit_plan": audit_row.get("ob_is_hit_plan"),
        "ob_is_executor_rpc": audit_row.get("ob_is_executor_rpc"),
        "ob_logical_reads": _safe_float(audit_row.get("ob_logical_reads")),
        "ob_physical_reads": _safe_float(audit_row.get("ob_physical_reads")),
        "ob_plan_text": explain_stdout if explain_ok else "",
        "ob_plan_rows": plan_rows,
        "ob_plan_error": explain_stderr if not explain_ok else "",
        "ob_plan_hash": compute_sql_id(explain_stdout) if explain_ok and explain_stdout else None,
        "replayed_at": started_at,
        "ob_stdout_preview": (stdout or "")[:500],
        "plan_monitor_rows": [],
    }
    merged = dict(workload_row)
    merged.update(replay_row)
    merged.update(derive_replay_metrics(merged))
    merged["plan_diff_signals"] = build_plan_diff_signals(merged)
    if (
        ok
        and config.settings.get("verify_results")
        and is_select_statement(rendered_sql)
    ):
        try:
            verification = perform_result_verification(config, workload_row, rendered_sql)
        except Exception as exc:
            verification = {
                "status": "skipped",
                "reason": "query_error:%s" % str(exc),
                "source_hash": "",
                "target_hash": "",
                "artifact_path": "",
                "mismatch_sample": [],
            }
        merged["verification_status"] = verification.get("status")
        merged["verification_reason"] = verification.get("reason")
        merged["verification_source_hash"] = verification.get("source_hash")
        merged["verification_target_hash"] = verification.get("target_hash")
        merged["verification_artifact_path"] = verification.get("artifact_path")
        merged["verification_mismatch_sample"] = verification.get("mismatch_sample") or []
    slowdown_threshold = float(config.settings.get("slowdown_threshold", DEFAULT_SLOWDOWN_THRESHOLD))
    if should_collect_plan_monitor(merged, slowdown_threshold):
        try:
            merged["plan_monitor_rows"] = collect_plan_monitor_rows(config, audit_row, rendered_sql)
        except Exception:
            LOG.exception("Plan monitor collection failed for sql_id=%s", workload_row.get("sql_id"))
            merged["plan_monitor_rows"] = []
        merged["plan_diff_signals"] = build_plan_diff_signals(merged)
    if ok and config.settings.get("plsql_profile") and is_plsql_statement(rendered_sql):
        try:
            plsql_profile = collect_plsql_profile(config, workload_row, rendered_sql, timeout_seconds)
        except Exception as exc:
            plsql_profile = {
                "status": "error",
                "error": str(exc),
                "runid": None,
                "top_lines": [],
                "artifact_path": "",
            }
        merged["plsql_profile_status"] = plsql_profile.get("status")
        merged["plsql_profile_error"] = plsql_profile.get("error")
        merged["plsql_profile_runid"] = plsql_profile.get("runid")
        merged["plsql_profile_artifact_path"] = plsql_profile.get("artifact_path")
        merged["plsql_profile_top_lines"] = plsql_profile.get("top_lines") or []
        merged["plsql_profile_unit_summary"] = plsql_profile.get("unit_summary") or []
        merged["plsql_profile_hot_blocks"] = plsql_profile.get("hot_blocks") or []
        merged["plsql_profile_diagnoses"] = plsql_profile.get("diagnoses") or []
        merged["plsql_profile_diagnosis_summary"] = plsql_profile.get("diagnosis_summary") or ""
        merged["plsql_profile_summary"] = summarize_plsql_profile(
            {
                "plsql_profile_status": plsql_profile.get("status"),
                "plsql_profile_top_lines": plsql_profile.get("top_lines") or [],
                "plsql_profile_diagnosis_summary": plsql_profile.get("diagnosis_summary") or "",
            }
        )
        merged["plsql_profile_mapping_summary"] = plsql_profile.get(
            "mapping_summary"
        ) or summarize_plsql_profile_mapping(
            {
                "plsql_profile_status": plsql_profile.get("status"),
                "plsql_profile_top_lines": plsql_profile.get("top_lines") or [],
            }
        )
    merged["recommendations"] = build_recommendations(
        merged, slowdown_threshold=slowdown_threshold
    )
    return merged


def replay_workload(config, workload_path, run_id):
    # type: (AppConfig, Union[str, Path], str) -> Path
    config.settings["_current_run_id"] = run_id
    replay_capabilities = probe_replay_capabilities(config)
    replay_capabilities["run_id"] = run_id
    capture_capabilities = probe_oracle_capabilities(config)
    capture_capabilities["run_id"] = run_id
    write_capability_files(config, run_id, capture_capabilities, replay_capabilities)
    workload_rows = read_jsonl(workload_path)
    if not workload_rows:
        raise ConfigError("Workload file is empty: %s" % str(workload_path))
    audit_dump_path = build_artifact_path("audit_dump", run_id, root_dir=config.settings["workloads_dir"])
    collector = None  # type: Optional[SQLAuditCollector]
    backend = ObclientReplayBackend()
    if replay_capabilities.get("sql_audit"):
        collector = SQLAuditCollector(config, audit_dump_path)
        collector.start()
    replay_rows = []
    try:
        for workload_row in workload_rows:
            replay_rows.append(
                replay_statement(config, workload_row, audit_collector=collector, backend=backend)
            )
    finally:
        if collector is not None:
            collector.stop()
    replay_path = build_artifact_path("replay", run_id, root_dir=config.settings["workloads_dir"])
    append_jsonl(replay_path, replay_rows)
    return replay_path


def _format_ratio(value):
    # type: (Optional[float]) -> str
    if value is None:
        return "n/a"
    return "%.3f" % value


def _render_svg_distribution_chart(rows, chart_id):
    # type: (List[Dict[str, Any]], str) -> str
    buckets = [
        ("Accelerated", "#2f855a", 0),
        ("Neutral", "#718096", 0),
        ("Mild Regression", "#dd6b20", 0),
        ("Severe Regression", "#c53030", 0),
        ("Failed", "#4a5568", 0),
    ]
    mutable = [list(item) for item in buckets]
    for row in rows:
        if row.get("ob_status") != "ok":
            mutable[4][2] += 1
            continue
        ratio = row.get("speedup_ratio")
        if ratio is None:
            mutable[1][2] += 1
        elif ratio >= 1.05:
            mutable[0][2] += 1
        elif ratio >= 0.95:
            mutable[1][2] += 1
        elif ratio >= 0.5:
            mutable[2][2] += 1
        else:
            mutable[3][2] += 1
    total = max(1, sum(item[2] for item in mutable))
    center_x = 90.0
    center_y = 90.0
    radius = 62.0
    legend_y = 24
    angle_start = -90.0
    path_fragments = []
    legend_fragments = []
    for idx, (label, color, count) in enumerate(mutable):
        fraction = float(count) / float(total)
        sweep = 360.0 * fraction
        angle_end = angle_start + sweep
        large_arc = 1 if sweep > 180.0 else 0
        start_x = center_x + radius * math.cos(math.radians(angle_start))
        start_y = center_y + radius * math.sin(math.radians(angle_start))
        end_x = center_x + radius * math.cos(math.radians(angle_end))
        end_y = center_y + radius * math.sin(math.radians(angle_end))
        if count > 0:
            path_fragments.append(
                '<path d="M {cx:.1f} {cy:.1f} L {sx:.1f} {sy:.1f} A {r:.1f} {r:.1f} 0 {arc} 1 {ex:.1f} {ey:.1f} Z" fill="{fill}"></path>'.format(
                    cx=center_x,
                    cy=center_y,
                    sx=start_x,
                    sy=start_y,
                    r=radius,
                    arc=large_arc,
                    ex=end_x,
                    ey=end_y,
                    fill=color,
                )
            )
        legend_fragments.append(
            '<g transform="translate(210,{y})"><rect width="12" height="12" fill="{fill}"></rect><text x="18" y="10" font-size="12">{label}: {count}</text></g>'.format(
                y=legend_y + idx * 22,
                fill=color,
                label=html.escape(label),
                count=count,
            )
        )
        angle_start = angle_end
    return (
        '<section id="overview-charts"><h2>Overview Charts</h2>'
        '<div id="{chart_id}" class="chart-block">'
        '<h3>Regression Distribution</h3>'
        '<svg viewBox="0 0 420 190" role="img" aria-label="distribution chart">'
        '{paths}<circle cx="90" cy="90" r="28" fill="#fff"></circle>'
        '{legend}</svg></div>'
    ).format(chart_id=chart_id, paths="".join(path_fragments), legend="".join(legend_fragments))


def _render_svg_timing_chart(rows, chart_id, source_only=False):
    # type: (List[Dict[str, Any]], str, bool) -> str
    selected = rows[: min(8, len(rows))]
    if not selected:
        return ""
    chart_width = 440
    chart_height = 48 + len(selected) * 32
    max_value = 1.0
    for row in selected:
        max_value = max(
            max_value,
            float(row.get("ob_elapsed_us") or 0.0),
            float(row.get("baseline_avg_elapsed_us") or row.get("oracle_avg_elapsed_us") or 0.0),
            float(row.get("source_total_elapsed_us") or 0.0),
        )
    bar_max_width = 220.0
    fragments = []
    for idx, row in enumerate(selected):
        y = 28 + idx * 32
        label = html.escape(str(row.get("sql_id")))
        if source_only:
            left_value = float(row.get("source_total_elapsed_us") or 0.0)
            right_value = float(row.get("ob_elapsed_us") or 0.0)
            left_label = "Total"
            right_label = "Avg"
        else:
            left_value = float(row.get("baseline_avg_elapsed_us") or row.get("oracle_avg_elapsed_us") or 0.0)
            right_value = float(row.get("ob_elapsed_us") or 0.0)
            left_label = "Oracle"
            right_label = "OB"
        left_width = (left_value / max_value) * bar_max_width
        right_width = (right_value / max_value) * bar_max_width
        fragments.append(
            '<text x="8" y="{y}" font-size="11">{label}</text>'
            '<rect x="120" y="{y1}" width="{lw:.1f}" height="10" fill="#3182ce"></rect>'
            '<rect x="120" y="{y2}" width="{rw:.1f}" height="10" fill="#dd6b20"></rect>'
            '<text x="{lx:.1f}" y="{y1t}" font-size="10">{ll}:{lv:.0f}</text>'
            '<text x="{rx:.1f}" y="{y2t}" font-size="10">{rl}:{rv:.0f}</text>'.format(
                y=y,
                label=label,
                y1=y - 10,
                y2=y + 4,
                lw=left_width,
                rw=right_width,
                lx=126 + left_width,
                rx=126 + right_width,
                y1t=y - 2,
                y2t=y + 14,
                ll=left_label,
                lv=left_value,
                rl=right_label,
                rv=right_value,
            )
        )
    return (
        '<div id="{chart_id}" class="chart-block"><h3>Timing Comparison</h3>'
        '<svg viewBox="0 0 {width} {height}" role="img" aria-label="timing chart">{content}</svg></div>'
        '</section>'
    ).format(chart_id=chart_id, width=chart_width, height=chart_height, content="".join(fragments))


def _render_svg_sql_source_chart(source_counts, chart_id):
    # type: (Dict[str, int], str) -> str
    if not source_counts:
        return ""
    ordered_items = sorted(source_counts.items(), key=lambda item: (-int(item[1]), item[0]))
    chart_width = 440
    chart_height = 56 + len(ordered_items) * 28
    max_value = max(1, max(int(count) for _, count in ordered_items))
    fragments = []
    for idx, (label, count) in enumerate(ordered_items):
        y = 30 + idx * 28
        bar_width = (float(count) / float(max_value)) * 220.0
        fragments.append(
            '<text x="8" y="{y}" font-size="11">{label}</text>'
            '<rect x="140" y="{y1}" width="{bw:.1f}" height="12" fill="#2b6cb0"></rect>'
            '<text x="{tx:.1f}" y="{ty}" font-size="10">{count}</text>'.format(
                y=y,
                label=html.escape(label),
                y1=y - 10,
                bw=bar_width,
                tx=146 + bar_width,
                ty=y,
                count=count,
            )
        )
    return (
        '<div id="{chart_id}" class="chart-block"><h3>SQL Text Source Distribution</h3>'
        '<svg viewBox="0 0 {width} {height}" role="img" aria-label="sql source chart">{content}</svg></div>'
    ).format(chart_id=chart_id, width=chart_width, height=chart_height, content="".join(fragments))


def generate_report_from_replay(config, replay_path, run_id, workload_path=None):
    # type: (AppConfig, Union[str, Path], str, Optional[Union[str, Path]]) -> Dict[str, Path]
    replay_rows = read_jsonl(replay_path)
    if not replay_rows:
        raise ConfigError("Replay file is empty: %s" % str(replay_path))
    if workload_path is None:
        workload_path = guess_workload_path_from_replay(replay_path)
    workload_index = load_workload_index(workload_path)
    enriched_rows = []
    for replay_row in replay_rows:
        base = dict(workload_index.get(str(replay_row.get("sql_id") or ""), {}))
        base.update(replay_row)
        base.update(derive_replay_metrics(base))
        base["plan_diff_signals"] = replay_row.get("plan_diff_signals") or build_plan_diff_signals(base)
        if "plsql_profile_summary" not in base:
            base["plsql_profile_summary"] = summarize_plsql_profile(base)
        if "plsql_profile_mapping_summary" not in base:
            base["plsql_profile_mapping_summary"] = summarize_plsql_profile_mapping(base)
        if "plsql_profile_diagnosis_summary" not in base:
            base["plsql_profile_diagnosis_summary"] = summarize_plsql_profile_diagnosis(base)
        base["recommendations"] = build_recommendations(
            base,
            slowdown_threshold=float(config.settings.get("slowdown_threshold", DEFAULT_SLOWDOWN_THRESHOLD)),
        )
        base["replay_workload_type"] = classify_replay_workload_type(base.get("sql_text"))
        enriched_rows.append(base)
    sort_key = lambda item: (
        item.get("speedup_ratio") is None,
        item.get("speedup_ratio") if item.get("speedup_ratio") is not None else 999999.0,
    )
    enriched_rows = sorted(enriched_rows, key=sort_key)
    top_n = int(config.settings.get("top_n", DEFAULT_TOP_N))
    selected_rows = enriched_rows[:top_n]
    slowdown_threshold = float(config.settings.get("slowdown_threshold", DEFAULT_SLOWDOWN_THRESHOLD))
    rolling_mode = bool(config.settings.get("_rolling_stream_report"))
    materialized_selected_rows = []
    for row in selected_rows:
        if (not rolling_mode) and has_external_diagnostics_config(config) and _should_collect_external_row_diagnostics(row, slowdown_threshold):
            try:
                row = _merge_external_diagnostics(
                    row,
                    collect_external_row_diagnostics(config, row, run_id),
                )
            except Exception:
                LOG.exception("External diagnostics collection failed for sql_id=%s", row.get("sql_id"))
        materialized_selected_rows.append(row)
    selected_rows = materialized_selected_rows
    report_dir = Path(config.settings["report_dir"])
    summary_path = build_artifact_path("report_summary", run_id, root_dir=report_dir)
    html_path = build_artifact_path("report_html", run_id, root_dir=report_dir)
    hints_path = build_artifact_path("report_hints", run_id, root_dir=report_dir)
    top_sql_rows = [row for row in selected_rows if str(row.get("replay_workload_type") or "sql") != "plsql"][:5]
    top_plsql_rows = [row for row in selected_rows if str(row.get("replay_workload_type") or "sql") == "plsql"][:5]

    summary_lines = [
        "Run ID: %s" % run_id,
        "Replay file: %s" % str(replay_path),
        "Report Mode: %s" % ("oracle-to-ob-rolling" if rolling_mode else "oracle-to-ob"),
        "Total statements: %d" % len(enriched_rows),
        "Successful statements: %d" % sum(1 for row in enriched_rows if row.get("ob_status") == "ok"),
        "Failed statements: %d" % sum(1 for row in enriched_rows if row.get("ob_status") != "ok"),
        "Verified matches: %d" % sum(1 for row in enriched_rows if row.get("verification_status") == "match"),
        "Verified mismatches: %d" % sum(1 for row in enriched_rows if row.get("verification_status") == "mismatch"),
        "Verified skipped: %d" % sum(1 for row in enriched_rows if row.get("verification_status") == "skipped"),
        "Observed workload types: sql=%d plsql=%d"
        % (
            sum(1 for row in enriched_rows if str(row.get("replay_workload_type") or "sql") != "plsql"),
            sum(1 for row in enriched_rows if str(row.get("replay_workload_type") or "sql") == "plsql"),
        ),
        "",
        "Top slow SQL:",
    ]
    for idx, row in enumerate(top_sql_rows, 1):
        summary_lines.append(
            "%d. sql_id=%s ob_us=%s sql=%s"
            % (
                idx,
                row.get("sql_id"),
                row.get("ob_elapsed_us"),
                build_sql_preview(row.get("sql_text"), limit=120),
            )
        )
    summary_lines.extend(
        [
            "",
            "Top slow PL/SQL:",
        ]
    )
    for idx, row in enumerate(top_plsql_rows, 1):
        summary_lines.append(
            "%d. sql_id=%s ob_us=%s plsql_diag=%s sql=%s"
            % (
                idx,
                row.get("sql_id"),
                row.get("ob_elapsed_us"),
                summarize_plsql_profile_diagnosis(row),
                build_sql_preview(row.get("sql_text"), limit=120),
            )
        )
    summary_lines.extend(
        [
        "",
        "Top regressions:",
    ]
    )
    for idx, row in enumerate(selected_rows, 1):
        rule_ids = ",".join(item["rule_id"] for item in row.get("recommendations", [])) or "none"
        verification_status = summarize_verification_evidence(row)
        plan_monitor_summary = summarize_plan_monitor_evidence(row)
        plan_risk_summary = summarize_plan_diff_signals(row)
        plsql_profile_summary = summarize_plsql_profile(row)
        plsql_profile_mapping_summary = summarize_plsql_profile_mapping(row)
        plsql_profile_diagnosis_summary = summarize_plsql_profile_diagnosis(row)
        external_summary = summarize_external_diagnostics(row)
        summary_line = (
            "%d. sql_id=%s speedup_ratio=%s baseline_us=%s ob_us=%s rules=%s verification=%s monitor=%s plan_risk=%s plsql=%s plsql_map=%s"
            % (
                idx,
                row.get("sql_id"),
                _format_ratio(row.get("speedup_ratio")),
                row.get("baseline_avg_elapsed_us") or row.get("oracle_avg_elapsed_us"),
                row.get("ob_elapsed_us"),
                rule_ids,
                verification_status,
                plan_monitor_summary,
                plan_risk_summary,
                plsql_profile_summary,
                plsql_profile_mapping_summary,
            )
        )
        if plsql_profile_diagnosis_summary != "n/a":
            summary_line = "%s plsql_diag=%s" % (summary_line, plsql_profile_diagnosis_summary)
        if external_summary != "n/a":
            summary_line = "%s external=%s" % (summary_line, external_summary)
        summary_lines.append(summary_line)
    write_text(summary_path, "\n".join(summary_lines) + "\n")

    html_rows = []
    for row in selected_rows:
        verification_status = summarize_verification_evidence(row)
        plan_monitor_summary = summarize_plan_monitor_evidence(row)
        plan_risk_summary = summarize_plan_diff_signals(row)
        plsql_profile_summary = summarize_plsql_profile(row)
        plsql_profile_mapping_summary = summarize_plsql_profile_mapping(row)
        plsql_profile_diagnosis_summary = summarize_plsql_profile_diagnosis(row)
        external_summary = summarize_external_diagnostics(row)
        evidence_parts = [
            "verification=%s" % verification_status,
            "monitor=%s" % plan_monitor_summary,
            "plan-risk=%s" % plan_risk_summary,
            "plsql=%s" % plsql_profile_summary,
        ]
        if plsql_profile_mapping_summary != "n/a":
            evidence_parts.append("plsql-map=%s" % plsql_profile_mapping_summary)
        if plsql_profile_diagnosis_summary != "n/a":
            evidence_parts.append("plsql-diag=%s" % plsql_profile_diagnosis_summary)
        if external_summary != "n/a":
            evidence_parts.append(external_summary)
        html_rows.append(
            "<tr><td>{sql_id}</td><td>{speedup}</td><td>{oracle}</td><td>{ob}</td><td>{rules}</td><td>{evidence}</td><td><pre>{sql}</pre></td></tr>".format(
                sql_id=html.escape(str(row.get("sql_id"))),
                speedup=html.escape(_format_ratio(row.get("speedup_ratio"))),
                oracle=html.escape(str(row.get("baseline_avg_elapsed_us") or row.get("oracle_avg_elapsed_us"))),
                ob=html.escape(str(row.get("ob_elapsed_us"))),
                rules=html.escape(
                    "%s | plan=%s | type=%s"
                    % (
                        ",".join(item["rule_id"] for item in row.get("recommendations", [])) or "none",
                        " > ".join(plan_row.get("operator", "") for plan_row in row.get("ob_plan_rows", [])[:4]) or "n/a",
                        row.get("ob_plan_type") or "n/a",
                    )
                ),
                evidence=html.escape(" | ".join(evidence_parts)),
                sql=html.escape(str(row.get("sql_text") or "")),
            )
        )
    slow_sql_rows_html = "".join(
        "<tr><td><a href=\"#sql-{sql_anchor}\">{sql_id}</a></td><td>{elapsed}</td><td><pre>{sql}</pre></td></tr>".format(
            sql_anchor=html.escape(str(row.get("sql_id"))),
            sql_id=html.escape(str(row.get("sql_id"))),
            elapsed=html.escape(str(row.get("ob_elapsed_us"))),
            sql=html.escape(build_sql_preview(row.get("sql_text"), limit=240)),
        )
        for row in top_sql_rows
    )
    slow_plsql_rows_html = "".join(
        "<tr><td><a href=\"#sql-{sql_anchor}\">{sql_id}</a></td><td>{elapsed}</td><td>{diag}</td><td><pre>{sql}</pre></td></tr>".format(
            sql_anchor=html.escape(str(row.get("sql_id"))),
            sql_id=html.escape(str(row.get("sql_id"))),
            elapsed=html.escape(str(row.get("ob_elapsed_us"))),
            diag=html.escape(summarize_plsql_profile_diagnosis(row)),
            sql=html.escape(build_sql_preview(row.get("sql_text"), limit=240)),
        )
        for row in top_plsql_rows
    )
    detail_cards_html = "".join(
        """<section id="sql-{sql_id}" class="detail-card">
<h3>SQL ID {sql_id}</h3>
<p><a href="#top">Back to Top</a></p>
<p>Type: {workload_type}</p>
<p>Speedup Ratio: {speedup}</p>
<p>Baseline Avg (us): {oracle}</p>
<p>OB Elapsed (us): {ob}</p>
<p>Evidence: {evidence}</p>
<details><summary>SQL Text</summary><pre>{sql_text}</pre></details>
</section>""".format(
            sql_id=html.escape(str(row.get("sql_id"))),
            workload_type=html.escape(str(row.get("replay_workload_type") or "sql")),
            speedup=html.escape(_format_ratio(row.get("speedup_ratio"))),
            oracle=html.escape(str(row.get("baseline_avg_elapsed_us") or row.get("oracle_avg_elapsed_us"))),
            ob=html.escape(str(row.get("ob_elapsed_us"))),
            evidence=html.escape(
                " | ".join(
                    ([
                        "verification=%s" % summarize_verification_evidence(row),
                        "monitor=%s" % summarize_plan_monitor_evidence(row),
                        "plan-risk=%s" % summarize_plan_diff_signals(row),
                        "plsql=%s" % summarize_plsql_profile(row),
                    ] + (
                        ["plsql-map=%s" % summarize_plsql_profile_mapping(row)]
                        if summarize_plsql_profile_mapping(row) != "n/a"
                        else []
                    ) + (
                        ["plsql-diag=%s" % summarize_plsql_profile_diagnosis(row)]
                        if summarize_plsql_profile_diagnosis(row) != "n/a"
                        else []
                    ) + (
                        [summarize_external_diagnostics(row)]
                        if summarize_external_diagnostics(row) != "n/a"
                        else []
                    ))
                )
            ),
            sql_text=html.escape(str(row.get("sql_text") or "")),
        )
        for row in selected_rows
    )
    charts_html = _render_svg_distribution_chart(enriched_rows, "distribution-chart")
    charts_html += _render_svg_timing_chart(selected_rows, "timing-chart", source_only=False)
    html_content = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>perf_comparator report</title>
<style>
body { font-family: Arial, sans-serif; margin: 24px; }
table { border-collapse: collapse; width: 100%%; }
th, td { border: 1px solid #ddd; padding: 8px; vertical-align: top; }
th { background: #f4f4f4; text-align: left; }
pre { white-space: pre-wrap; margin: 0; }
.nav-block { border: 1px solid #cbd5e0; background: #f7fafc; padding: 12px; margin: 0 0 20px 0; }
.detail-card { border-top: 2px solid #cbd5e0; padding-top: 16px; margin-top: 20px; }
.chart-block { margin-bottom: 20px; }
svg text { font-family: Arial, sans-serif; fill: #1a202c; }
</style></head><body>
<a id="top"></a><h1>perf_comparator report</h1>
<p>Run ID: %s</p>
<p>Mode: %s</p>
<div id="report-nav" class="nav-block"><h2>Navigation</h2><ul><li><a href="#overview-charts">Overview</a></li><li><a href="#slow-sql-section">Top Slow SQL</a></li><li><a href="#slow-plsql-section">Top Slow PL/SQL</a></li><li><a href="#detailed-findings">Detailed Findings</a></li></ul></div>
%s
<div id="slow-sql-section" class="chart-block">
<h2>Top Slow SQL</h2>
<table><thead><tr><th>SQL ID</th><th>OB Elapsed (us)</th><th>SQL</th></tr></thead><tbody>%s</tbody></table>
</div>
<div id="slow-plsql-section" class="chart-block">
<h2>Top Slow PL/SQL</h2>
<table><thead><tr><th>SQL ID</th><th>OB Elapsed (us)</th><th>Diagnosis</th><th>PL/SQL</th></tr></thead><tbody>%s</tbody></table>
</div>
<section id="detailed-findings"><h2>Detailed Findings</h2>%s</section></body></html>
""" % (
        html.escape(run_id),
        html.escape("oracle-to-ob-rolling" if rolling_mode else "oracle-to-ob"),
        charts_html,
        slow_sql_rows_html,
        slow_plsql_rows_html,
        detail_cards_html,
    )
    write_text(html_path, html_content)

    hints_lines = ["-- perf_comparator recommendations", "-- run_id: %s" % run_id, ""]
    hints_lines.append("-- slow_sql:")
    for row in top_sql_rows:
        hints_lines.append(
            "-- sql_id=%s ob_elapsed_us=%s"
            % (row.get("sql_id"), row.get("ob_elapsed_us"))
        )
    hints_lines.append("-- slow_plsql:")
    for row in top_plsql_rows:
        hints_lines.append(
            "-- sql_id=%s ob_elapsed_us=%s diagnosis=%s"
            % (
                row.get("sql_id"),
                row.get("ob_elapsed_us"),
                summarize_plsql_profile_diagnosis(row),
            )
        )
    hints_lines.append("")
    for row in selected_rows:
        hints_lines.append("-- sql_id: %s" % row.get("sql_id"))
        if row.get("verification_status"):
            hints_lines.append(
                "-- verification: %s" % summarize_verification_evidence(row)
            )
        if row.get("plan_monitor_rows"):
            hints_lines.append(
                "-- plan-monitor: %s" % summarize_plan_monitor_evidence(row)
            )
        if row.get("plan_diff_signals"):
            hints_lines.append(
                "-- plan-risk: %s" % summarize_plan_diff_signals(row)
            )
        if row.get("plsql_profile_status"):
            hints_lines.append(
                "-- plsql-profile: %s" % summarize_plsql_profile(row)
            )
            hints_lines.append(
                "-- plsql-profile-map: %s" % summarize_plsql_profile_mapping(row)
            )
            if summarize_plsql_profile_diagnosis(row) != "n/a":
                hints_lines.append(
                    "-- plsql-profile-diagnosis: %s" % summarize_plsql_profile_diagnosis(row)
                )
        external_summary = summarize_external_diagnostics(row)
        if external_summary != "n/a":
            hints_lines.append("-- external-diagnostics: %s" % external_summary)
        for item in row.get("recommendations", []):
            hints_lines.append("-- %s: %s" % (item["rule_id"], item["message"]))
            hints_lines.append(item["hint_sql"])
        hints_lines.append("")
    write_text(hints_path, "\n".join(hints_lines).rstrip() + "\n")
    return {"summary": summary_path, "html": html_path, "hints": hints_path}


def generate_report_from_source_workload(config, workload_path, run_id):
    # type: (AppConfig, Union[str, Path], str) -> Dict[str, Path]
    workload_rows = read_jsonl(workload_path)
    if not workload_rows:
        raise ConfigError("Source workload file is empty: %s" % str(workload_path))
    workload_rows, sql_text_stats = backfill_source_workload_sql_texts(config, workload_rows)
    workload_rows = [
        row
        for row in workload_rows
        if not is_internal_perf_comparator_source_sql(row.get("sql_text"))
    ]
    if not workload_rows:
        raise ConfigError("Source workload only contained internal perf_comparator source queries")
    actor_fields = get_source_actor_fields(config)
    for row in workload_rows:
        if not row.get("source_actor_fields"):
            row["source_actor_fields"] = list(actor_fields)
    enriched_rows = aggregate_ob_source_workload_rows(workload_rows)
    if not enriched_rows:
        raise ConfigError("Source workload did not produce any reportable rows")
    visible_sql_stmt_count = sum(
        1 for row in enriched_rows if not is_missing_sql_text(row.get("sql_text"))
    )
    missing_sql_stmt_count = max(0, len(enriched_rows) - visible_sql_stmt_count)
    visibility_warning = build_query_sql_visibility_warning(config)
    sql_source_counts = compute_sql_text_source_distribution(enriched_rows)
    actor_summaries = compute_source_actor_summaries(enriched_rows)
    caller_scope = str(actor_summaries[0].get("attribution_scope") or "fallback") if actor_summaries else "fallback"
    direct_stmt_count = sum(
        1 for row in enriched_rows if str(row.get("source_attribution_quality") or "") in ("direct", "mixed")
    )
    fallback_stmt_count = sum(
        1 for row in enriched_rows if str(row.get("source_attribution_quality") or "") == "fallback"
    )
    rolling_mode = bool(config.settings.get("_rolling_source_report"))
    slowdown_threshold = float(config.settings.get("slowdown_threshold", DEFAULT_SLOWDOWN_THRESHOLD))
    for row in enriched_rows:
        if not rolling_mode and should_collect_plan_monitor(row, slowdown_threshold):
            try:
                row["plan_monitor_rows"] = collect_source_plan_monitor_rows(config, row)
            except Exception:
                LOG.exception("Source plan monitor collection failed for sql_id=%s", row.get("sql_id"))
                row["plan_monitor_rows"] = []
        row["plan_diff_signals"] = build_plan_diff_signals(row)
        row["plsql_profile_summary"] = summarize_plsql_profile(row)
        row["plsql_profile_mapping_summary"] = summarize_plsql_profile_mapping(row)
        row["recommendations"] = build_recommendations(row, slowdown_threshold=slowdown_threshold)

    enriched_rows = sorted(
        enriched_rows,
        key=lambda item: (
            -(float(item.get("source_total_elapsed_us") or 0.0)),
            -(int(item.get("source_sample_count") or 0)),
            -(float(item.get("ob_elapsed_us") or 0.0)),
        ),
    )
    top_n = int(config.settings.get("top_n", DEFAULT_TOP_N))
    selected_rows = enriched_rows[:top_n]
    materialized_selected_rows = []
    for row in selected_rows:
        if (not rolling_mode) and has_external_diagnostics_config(config) and _should_collect_external_row_diagnostics(row, slowdown_threshold):
            try:
                row = _merge_external_diagnostics(
                    row,
                    collect_external_row_diagnostics(config, row, run_id),
                )
            except Exception:
                LOG.exception("External diagnostics collection failed for source sql_id=%s", row.get("sql_id"))
        materialized_selected_rows.append(row)
    selected_rows = materialized_selected_rows
    report_dir = Path(config.settings["report_dir"])
    summary_path = build_artifact_path("report_summary", run_id, root_dir=report_dir)
    html_path = build_artifact_path("report_html", run_id, root_dir=report_dir)
    hints_path = build_artifact_path("report_hints", run_id, root_dir=report_dir)
    top_sql_rows = [row for row in selected_rows if str(row.get("source_workload_type") or "sql") != "plsql"][:5]
    top_plsql_rows = [row for row in selected_rows if str(row.get("source_workload_type") or "sql") == "plsql"][:5]

    summary_lines = [
        "Run ID: %s" % run_id,
        "Source workload file: %s" % str(workload_path),
        "Report Mode: source-only",
        "Total statements: %d" % len(enriched_rows),
        "Captured samples: %d" % sum(int(row.get("source_sample_count") or 0) for row in enriched_rows),
        "Distributed statements: %d" % sum(1 for row in enriched_rows if str(row.get("ob_plan_type_raw") or "") == "3"),
        "SQL text coverage: %d/%d visible (row_backfilled=%d row_missing=%d via=%s)"
        % (
            visible_sql_stmt_count,
            len(enriched_rows),
            int(sql_text_stats.get("backfilled", 0)),
            int(sql_text_stats.get("missing", 0)),
            sql_text_stats.get("lookup_user") or "unconfigured",
        ),
        "SQL text recovery detail: local=%d ocp_native=%d ocp_template=%d"
        % (
            int(sql_source_counts.get("source_sys", 0)),
            int(sql_source_counts.get("ocp_native", 0)),
            int(sql_source_counts.get("ocp_template", 0)),
        ),
    ]
    if visibility_warning:
        summary_lines.extend(
            [
                visibility_warning,
                "",
            ]
        )
    summary_lines.extend(
        [
        "Observed caller groups: %d (fields=%s)"
        % (
            len(actor_summaries),
            ",".join(get_source_actor_fields(config)),
        ),
        "Caller attribution coverage: direct_or_mixed=%d fallback_only=%d top_callers_scope=%s"
        % (direct_stmt_count, fallback_stmt_count, caller_scope),
        "Observed workload types: sql=%d plsql=%d"
        % (
            sum(1 for row in enriched_rows if str(row.get("source_workload_type") or "sql") != "plsql"),
            sum(1 for row in enriched_rows if str(row.get("source_workload_type") or "sql") == "plsql"),
        ),
        "",
        "Top caller groups:",
    ]
    )
    for idx, actor in enumerate(actor_summaries[:5], 1):
        summary_lines.append(
            "%d. actor=%s samples=%s total_elapsed_us=%s statements=%s sql=%s plsql=%s"
            % (
                idx,
                actor.get("actor"),
                actor.get("sample_count"),
                int(actor.get("total_elapsed_us") or 0.0),
                actor.get("statement_count"),
                actor.get("sql_count"),
                actor.get("plsql_count"),
            )
        )
    summary_lines.extend(
        [
            "",
            "Top slow SQL:",
        ]
    )
    for idx, row in enumerate(top_sql_rows, 1):
        summary_lines.append(
            "%d. actor=%s sql_id=%s avg_elapsed_us=%s attribution=%s cause=%s sql=%s"
            % (
                idx,
                row.get("source_primary_actor") or "unattributed",
                row.get("sql_id"),
                row.get("ob_elapsed_us"),
                summarize_source_attribution(row),
                summarize_source_likely_cause(row),
                build_sql_preview(row.get("sql_text"), limit=120),
            )
        )
    summary_lines.extend(
        [
            "",
            "Top slow PL/SQL:",
        ]
    )
    for idx, row in enumerate(top_plsql_rows, 1):
        summary_lines.append(
            "%d. actor=%s sql_id=%s avg_elapsed_us=%s attribution=%s cause=%s sql=%s"
            % (
                idx,
                row.get("source_primary_actor") or "unattributed",
                row.get("sql_id"),
                row.get("ob_elapsed_us"),
                summarize_source_attribution(row),
                summarize_source_likely_cause(row),
                build_sql_preview(row.get("sql_text"), limit=120),
            )
        )
    summary_lines.extend(
        [
            "",
        "Top hotspots:",
        ]
    )
    if missing_sql_stmt_count > 0:
        summary_lines.append(
            "SQL text note: ordinary users may not see QUERY_SQL on OB 4.2.5; configure [%s] and enable _enable_sql_audit_query_sql=true."
            % SECTION_OCEANBASE_SOURCE_SYS
        )
        summary_lines.append("")
    for idx, row in enumerate(selected_rows, 1):
        rule_ids = ",".join(item["rule_id"] for item in row.get("recommendations", [])) or "none"
        external_summary = summarize_external_diagnostics(row)
        summary_line = (
            "%d. sql_id=%s type=%s actor=%s attribution=%s samples=%s avg_elapsed_us=%s total_elapsed_us=%s rules=%s monitor=%s plan_risk=%s cause=%s"
            % (
                idx,
                row.get("sql_id"),
                row.get("source_workload_type") or "sql",
                row.get("source_primary_actor") or "unattributed",
                summarize_source_attribution(row),
                row.get("source_sample_count"),
                row.get("ob_elapsed_us"),
                row.get("source_total_elapsed_us"),
                rule_ids,
                summarize_plan_monitor_evidence(row),
                summarize_plan_diff_signals(row),
                summarize_source_likely_cause(row),
            )
        )
        if external_summary != "n/a":
            summary_line = "%s external=%s" % (summary_line, external_summary)
        summary_lines.append(summary_line)
        summary_lines.append(
            "   sql: [%s] %s"
            % (
                row.get("source_sql_text_source") or row.get("source_sql_text_status") or "unknown",
                build_sql_preview(row.get("sql_text")),
            )
        )
    write_text(summary_path, "\n".join(summary_lines) + "\n")

    html_rows = []
    for row in selected_rows:
        external_summary = summarize_external_diagnostics(row)
        evidence_parts = [
            "actor=%s" % (row.get("source_primary_actor") or "unattributed"),
            "attribution=%s" % summarize_source_attribution(row),
            "type=%s" % (row.get("source_workload_type") or "sql"),
            "cause=%s" % summarize_source_likely_cause(row),
            "monitor=%s" % summarize_plan_monitor_evidence(row),
            "plan-risk=%s" % summarize_plan_diff_signals(row),
            "queue_us=%s" % row.get("ob_queue_time_us"),
            "retry=%s" % row.get("ob_retry_cnt"),
        ]
        if external_summary != "n/a":
            evidence_parts.append(external_summary)
        html_rows.append(
            "<tr><td>{sql_id}</td><td>{samples}</td><td>{avg_elapsed}</td><td>{total_elapsed}</td><td>{rules}</td><td>{evidence}</td><td><pre>{sql}</pre></td></tr>".format(
                sql_id=html.escape(str(row.get("sql_id"))),
                samples=html.escape(str(row.get("source_sample_count"))),
                avg_elapsed=html.escape(str(row.get("ob_elapsed_us"))),
                total_elapsed=html.escape(str(row.get("source_total_elapsed_us"))),
                rules=html.escape(
                    "%s | type=%s | net_ratio=%s"
                    % (
                        ",".join(item["rule_id"] for item in row.get("recommendations", [])) or "none",
                        row.get("ob_plan_type") or "n/a",
                        _format_ratio(row.get("net_ratio")),
                    )
                ),
                evidence=html.escape(" | ".join(evidence_parts)),
                sql=html.escape(build_sql_preview(row.get("sql_text"), limit=400)),
            )
        )
    caller_rows_html = "".join(
        "<tr><td>{actor}</td><td>{scope}</td><td>{samples}</td><td>{elapsed}</td><td>{statements}</td><td>{sql_count}</td><td>{plsql_count}</td></tr>".format(
            actor=html.escape(str(actor.get("actor"))),
            scope=html.escape(str(actor.get("attribution_scope") or "fallback")),
            samples=html.escape(str(actor.get("sample_count"))),
            elapsed=html.escape(str(int(actor.get("total_elapsed_us") or 0.0))),
            statements=html.escape(str(actor.get("statement_count"))),
            sql_count=html.escape(str(actor.get("sql_count"))),
            plsql_count=html.escape(str(actor.get("plsql_count"))),
        )
        for actor in actor_summaries[:8]
    )
    slow_sql_rows_html = "".join(
        "<tr><td>{actor}</td><td><a href=\"#sql-{sql_anchor}\">{sql_id}</a></td><td>{elapsed}</td><td>{attribution}</td><td>{cause}</td><td><pre>{sql}</pre></td></tr>".format(
            actor=html.escape(str(row.get("source_primary_actor") or "unattributed")),
            sql_anchor=html.escape(str(row.get("sql_id"))),
            sql_id=html.escape(str(row.get("sql_id"))),
            elapsed=html.escape(str(row.get("ob_elapsed_us"))),
            attribution=html.escape(summarize_source_attribution(row)),
            cause=html.escape(summarize_source_likely_cause(row)),
            sql=html.escape(build_sql_preview(row.get("sql_text"), limit=240)),
        )
        for row in top_sql_rows
    )
    slow_plsql_rows_html = "".join(
        "<tr><td>{actor}</td><td><a href=\"#sql-{sql_anchor}\">{sql_id}</a></td><td>{elapsed}</td><td>{attribution}</td><td>{cause}</td><td><pre>{sql}</pre></td></tr>".format(
            actor=html.escape(str(row.get("source_primary_actor") or "unattributed")),
            sql_anchor=html.escape(str(row.get("sql_id"))),
            sql_id=html.escape(str(row.get("sql_id"))),
            elapsed=html.escape(str(row.get("ob_elapsed_us"))),
            attribution=html.escape(summarize_source_attribution(row)),
            cause=html.escape(summarize_source_likely_cause(row)),
            sql=html.escape(build_sql_preview(row.get("sql_text"), limit=240)),
        )
        for row in top_plsql_rows
    )
    detail_cards_html = "".join(
        """<section id="sql-{sql_id}" class="detail-card">
<h3>SQL ID {sql_id}</h3>
<p><a href="#top">Back to Top</a></p>
<p>Actor: {actor}</p>
<p>Attribution: {attribution}</p>
<p>Type: {workload_type}</p>
<p>Samples: {samples}</p>
<p>Avg Elapsed (us): {avg_elapsed}</p>
<p>Total Elapsed (us): {total_elapsed}</p>
<p>Cause: {cause}</p>
<p>Evidence: {evidence}</p>
<details><summary>SQL Text</summary><pre>{sql_text}</pre></details>
</section>""".format(
            sql_id=html.escape(str(row.get("sql_id"))),
            actor=html.escape(str(row.get("source_primary_actor") or "unattributed")),
            attribution=html.escape(summarize_source_attribution(row)),
            workload_type=html.escape(str(row.get("source_workload_type") or "sql")),
            samples=html.escape(str(row.get("source_sample_count"))),
            avg_elapsed=html.escape(str(row.get("ob_elapsed_us"))),
            total_elapsed=html.escape(str(row.get("source_total_elapsed_us"))),
            cause=html.escape(summarize_source_likely_cause(row)),
            evidence=html.escape(
                " | ".join(
                    [
                        "monitor=%s" % summarize_plan_monitor_evidence(row),
                        "plan-risk=%s" % summarize_plan_diff_signals(row),
                        "rules=%s" % (",".join(item["rule_id"] for item in row.get("recommendations", [])) or "none"),
                    ]
                )
            ),
            sql_text=html.escape(str(row.get("sql_text") or "")),
        )
        for row in selected_rows
    )
    charts_html = _render_svg_distribution_chart(enriched_rows, "source-distribution-chart")
    charts_html += _render_svg_sql_source_chart(sql_source_counts, "sql-source-chart")
    charts_html += _render_svg_timing_chart(selected_rows, "source-timing-chart", source_only=True)
    warning_html = ""
    if visibility_warning:
        warning_html = (
            '<div id="query-sql-visibility-warning" class="warning-block"><strong>%s</strong></div>'
            % html.escape(visibility_warning)
        )
    html_content = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>perf_comparator source report</title>
<style>
body { font-family: Arial, sans-serif; margin: 24px; }
table { border-collapse: collapse; width: 100%%; }
th, td { border: 1px solid #ddd; padding: 8px; vertical-align: top; }
th { background: #f4f4f4; text-align: left; }
pre { white-space: pre-wrap; margin: 0; }
 .chart-block { margin-bottom: 20px; }
 .nav-block { border: 1px solid #cbd5e0; background: #f7fafc; padding: 12px; margin: 0 0 20px 0; }
 .detail-card { border-top: 2px solid #cbd5e0; padding-top: 16px; margin-top: 20px; }
 .warning-block { border: 1px solid #c53030; background: #fff5f5; color: #742a2a; padding: 12px; margin: 0 0 20px 0; }
 svg text { font-family: Arial, sans-serif; fill: #1a202c; }
</style></head><body>
<a id="top"></a><h1>perf_comparator source-only report</h1>
<p>Run ID: %s</p>
<p>Mode: source-only</p>
<p>Caller fields: %s</p>
<div id="report-nav" class="nav-block"><h2>Navigation</h2><ul><li><a href="#overview-charts">Overview</a></li><li><a href="#top-caller-groups">Top Caller Groups</a></li><li><a href="#slow-sql-section">Top Slow SQL</a></li><li><a href="#slow-plsql-section">Top Slow PL/SQL</a></li><li><a href="#detailed-findings">Detailed Findings</a></li></ul></div>
%s
%s
<div id="top-caller-groups" class="chart-block">
<h2>Top Caller Groups</h2>
<table><thead><tr><th>Actor</th><th>Scope</th><th>Samples</th><th>Total Elapsed (us)</th><th>Statements</th><th>SQL</th><th>PL/SQL</th></tr></thead><tbody>%s</tbody></table>
</div>
<div id="slow-sql-section" class="chart-block">
<h2>Top Slow SQL</h2>
<table><thead><tr><th>Actor</th><th>SQL ID</th><th>Avg Elapsed (us)</th><th>Attribution</th><th>Cause</th><th>SQL</th></tr></thead><tbody>%s</tbody></table>
</div>
<div id="slow-plsql-section" class="chart-block">
<h2>Top Slow PL/SQL</h2>
<table><thead><tr><th>Actor</th><th>SQL ID</th><th>Avg Elapsed (us)</th><th>Attribution</th><th>Cause</th><th>PL/SQL</th></tr></thead><tbody>%s</tbody></table>
</div>
<section id="detailed-findings"><h2>Detailed Findings</h2>%s</section></body></html>
""" % (
        html.escape(run_id),
        html.escape(",".join(get_source_actor_fields(config))),
        warning_html,
        charts_html,
        caller_rows_html,
        slow_sql_rows_html,
        slow_plsql_rows_html,
        detail_cards_html,
    )
    write_text(html_path, html_content)

    hints_lines = ["-- perf_comparator source-only recommendations", "-- run_id: %s" % run_id, ""]
    hints_lines.append("-- caller_fields: %s" % ",".join(get_source_actor_fields(config)))
    hints_lines.append(
        "-- sql_text_coverage: visible=%s total=%s row_backfilled=%s row_missing=%s via=%s"
        % (
            visible_sql_stmt_count,
            len(enriched_rows),
            int(sql_text_stats.get("backfilled", 0)),
            int(sql_text_stats.get("missing", 0)),
            sql_text_stats.get("lookup_user") or "unconfigured",
        )
    )
    hints_lines.append(
        "-- sql_text_recovery_detail: local=%s ocp_native=%s ocp_template=%s"
        % (
            int(sql_source_counts.get("source_sys", 0)),
            int(sql_source_counts.get("ocp_native", 0)),
            int(sql_source_counts.get("ocp_template", 0)),
        )
    )
    if visibility_warning:
        hints_lines.append("-- sql_visibility_warning: %s" % visibility_warning)
    if missing_sql_stmt_count > 0:
        hints_lines.append(
            "-- sql_text_note: configure [%s] and _enable_sql_audit_query_sql=true when QUERY_SQL is hidden from ordinary users"
            % SECTION_OCEANBASE_SOURCE_SYS
        )
    hints_lines.append("")
    hints_lines.append("-- top_callers:")
    for actor in actor_summaries[:5]:
        hints_lines.append(
            "-- actor=%s samples=%s total_elapsed_us=%s statements=%s sql=%s plsql=%s"
            % (
                actor.get("actor"),
                actor.get("sample_count"),
                int(actor.get("total_elapsed_us") or 0.0),
                actor.get("statement_count"),
                actor.get("sql_count"),
                actor.get("plsql_count"),
            )
        )
    hints_lines.append("-- slow_sql:")
    for row in top_sql_rows:
        hints_lines.append(
            "-- actor=%s sql_id=%s avg_elapsed_us=%s cause=%s"
            % (
                row.get("source_primary_actor") or "unattributed",
                row.get("sql_id"),
                row.get("ob_elapsed_us"),
                summarize_source_likely_cause(row),
            )
        )
    hints_lines.append("-- slow_plsql:")
    for row in top_plsql_rows:
        hints_lines.append(
            "-- actor=%s sql_id=%s avg_elapsed_us=%s cause=%s"
            % (
                row.get("source_primary_actor") or "unattributed",
                row.get("sql_id"),
                row.get("ob_elapsed_us"),
                summarize_source_likely_cause(row),
            )
        )
    hints_lines.append("")
    for row in selected_rows:
        hints_lines.append("-- sql_id: %s" % row.get("sql_id"))
        hints_lines.append("-- samples: %s" % row.get("source_sample_count"))
        hints_lines.append("-- actor: %s" % (row.get("source_primary_actor") or "unattributed"))
        hints_lines.append("-- attribution: %s" % summarize_source_attribution(row))
        hints_lines.append("-- workload_type: %s" % (row.get("source_workload_type") or "sql"))
        hints_lines.append("-- cause: %s" % summarize_source_likely_cause(row))
        hints_lines.append(
            "-- sql_text_source: %s"
            % (row.get("source_sql_text_source") or row.get("source_sql_text_status") or "unknown")
        )
        hints_lines.append("-- sql_preview: %s" % build_sql_preview(row.get("sql_text"), limit=240))
        hints_lines.append("-- monitor: %s" % summarize_plan_monitor_evidence(row))
        hints_lines.append("-- plan-risk: %s" % summarize_plan_diff_signals(row))
        external_summary = summarize_external_diagnostics(row)
        if external_summary != "n/a":
            hints_lines.append("-- external-diagnostics: %s" % external_summary)
        for item in row.get("recommendations", []):
            hints_lines.append("-- %s: %s" % (item["rule_id"], item["message"]))
            hints_lines.append(item["hint_sql"])
        hints_lines.append("")
    write_text(hints_path, "\n".join(hints_lines).rstrip() + "\n")
    return {"summary": summary_path, "html": html_path, "hints": hints_path}


def clone_app_config(
    config,
    oracle_source=None,
    oceanbase_source=None,
    oceanbase_source_sys=None,
    oceanbase_target=None,
    settings_updates=None,
    config_path=None,
):
    # type: (AppConfig, Optional[Dict[str, str]], Optional[Dict[str, str]], Optional[Dict[str, str]], Optional[Dict[str, str]], Optional[Dict[str, Any]], Optional[str]) -> AppConfig
    settings = dict(config.settings)
    if settings_updates:
        settings.update(settings_updates)
    return AppConfig(
        oracle_source=dict(oracle_source if oracle_source is not None else config.oracle_source),
        oceanbase_source=dict(oceanbase_source if oceanbase_source is not None else config.oceanbase_source),
        oceanbase_source_sys=dict(
            oceanbase_source_sys
            if oceanbase_source_sys is not None
            else config.oceanbase_source_sys
        ),
        oceanbase_target=dict(oceanbase_target if oceanbase_target is not None else config.oceanbase_target),
        settings=settings,
        config_path=str(config_path or config.config_path),
    )


def resolve_realdb_reference_path(explicit_path, default_path):
    # type: (Optional[str], str) -> Optional[str]
    candidate = str(explicit_path or "").strip()
    if candidate:
        if Path(candidate).exists():
            return candidate
        return None
    if Path(default_path).exists():
        return default_path
    return None


def load_runtime_reference_config(path):
    # type: (str) -> Optional[AppConfig]
    if not path:
        return None
    try:
        return load_config(path)
    except Exception:
        LOG.exception("Failed to load runtime reference config: %s", path)
        return None


def resolve_realdb_oracle_config(config, args):
    # type: (AppConfig, argparse.Namespace) -> AppConfig
    if config.oracle_source:
        return config
    reference_path = resolve_realdb_reference_path(
        getattr(args, "realdb_oracle_config", None), DEFAULT_REALDB_ORACLE_CONFIG
    )
    reference_config = load_runtime_reference_config(reference_path) if reference_path else None
    if reference_config and reference_config.oracle_source:
        return clone_app_config(
            config,
            oracle_source=reference_config.oracle_source,
            config_path=reference_config.config_path,
        )
    return config


def resolve_realdb_ob_source_config(config, args):
    # type: (AppConfig, argparse.Namespace) -> AppConfig
    if config.oceanbase_source:
        return config
    reference_path = str(getattr(args, "realdb_ob_source_config", None) or "").strip()
    if not reference_path:
        return config
    reference_config = load_runtime_reference_config(reference_path) if reference_path else None
    if reference_config and reference_config.oceanbase_source:
        return clone_app_config(
            config,
            oceanbase_source=reference_config.oceanbase_source,
            settings_updates={"source_db_mode": SOURCE_DB_MODE_OCEANBASE},
            config_path=reference_config.config_path,
        )
    return config


def probe_ob_source_audit_capability(config):
    # type: (AppConfig) -> Tuple[bool, str]
    if not config.oceanbase_source:
        return False, "ob_source_not_configured"
    ok, stdout, stderr = _obclient_run_sql_on_source(
        config,
        "SELECT REQUEST_ID FROM GV$OB_SQL_AUDIT WHERE ROWNUM = 1",
        timeout=config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT),
        session_query_timeout_us=config.settings.get(
            "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
        ),
    )
    if ok:
        return True, ""
    return False, (stderr or stdout or "source_sql_audit_unavailable")


def _build_realdb_probe_sql():
    # type: () -> str
    return "SELECT 1 AS PERF_COMPARATOR_PROBE, 'PING' AS PERF_COMPARATOR_TAG FROM dual"


def _split_ddl_blocks(sql_text):
    # type: (str) -> List[str]
    blocks = re.split(r"(?m)^\s*/\s*$", str(sql_text or ""))
    return [block.strip() for block in blocks if block.strip()]


def deploy_profile_test_package(config, package_sql=None):
    # type: (AppConfig, Optional[str]) -> None
    sql_text = package_sql or PROFILER_TEST_PACKAGE_SQL
    for block in _split_ddl_blocks(sql_text):
        ok, stdout, stderr = obclient_run_sql(
            config.oceanbase_target,
            block,
            timeout=config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT),
            session_query_timeout_us=config.settings.get(
                "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
            ),
        )
        if not ok:
            raise ConfigError("Failed to deploy profiler test package: %s" % (stderr or stdout))


def cleanup_profile_test_package(config):
    # type: (AppConfig) -> None
    obclient_run_sql(
        config.oceanbase_target,
        "DROP PACKAGE %s" % PROFILER_TEST_PACKAGE_NAME,
        timeout=config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT),
        session_query_timeout_us=config.settings.get(
            "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
        ),
    )


def run_realdb_oracle_smoke(config, args, run_id):
    # type: (AppConfig, argparse.Namespace, str) -> Dict[str, Any]
    smoke_config = resolve_realdb_oracle_config(config, args)
    if not smoke_config.oracle_source:
        return {"step": "oracle_replay_smoke", "status": "skipped", "reason": "oracle_not_configured"}
    connection = _open_oracle_connection(smoke_config)
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT 1 FROM dual")
        cursor.fetchone()
        cursor.close()
    finally:
        connection.close()
    smoke_run_id = "%s_oracle" % run_id
    smoke_sql_path = Path(smoke_config.settings["workloads_dir"]) / ("realdb_probe_%s.sql" % smoke_run_id)
    write_text(smoke_sql_path, _build_realdb_probe_sql() + ";\n")
    smoke_runtime_config = clone_app_config(
        smoke_config,
        settings_updates={
            "verify_results": True,
            "result_sample_limit": min(
                int(smoke_config.settings.get("result_sample_limit", 10000) or 10000), 100
            ),
            "_current_run_id": smoke_run_id,
        },
    )
    workload_path = capture_from_sql_file(smoke_runtime_config, str(smoke_sql_path), smoke_run_id)
    replay_path = replay_workload(smoke_runtime_config, workload_path, smoke_run_id)
    report_paths = generate_report_from_replay(smoke_runtime_config, replay_path, smoke_run_id, workload_path)
    replay_rows = read_jsonl(replay_path)
    verification_status = replay_rows[0].get("verification_status") if replay_rows else None
    return {
        "step": "oracle_replay_smoke",
        "status": "passed",
        "workload_path": str(workload_path),
        "replay_path": str(replay_path),
        "summary_path": str(report_paths["summary"]),
        "verification_status": verification_status,
    }


def run_realdb_profiler_smoke(config, args, run_id):
    # type: (AppConfig, argparse.Namespace, str) -> Dict[str, Any]
    if not getattr(args, "realdb_deploy_profile_package", False):
        return {"step": "plsql_profiler_smoke", "status": "skipped", "reason": "package_deploy_not_enabled"}
    package_sql = None
    if getattr(args, "realdb_profile_package_sql", None):
        package_sql = Path(args.realdb_profile_package_sql).read_text(encoding="utf-8")
    try:
        deploy_profile_test_package(config, package_sql=package_sql)
    except Exception as exc:
        return {
            "step": "plsql_profiler_smoke",
            "status": "skipped",
            "reason": "package_deploy_failed",
            "error": str(exc),
        }
    try:
        profile_call = (
            getattr(args, "realdb_profile_package_call", None)
            or DEFAULT_REALDB_PROFILER_CALL
        )
        profile_config = clone_app_config(
            config,
            settings_updates={
                "plsql_profile": True,
                "_current_run_id": "%s_profiler" % run_id,
            },
        )
        replay_row = replay_statement(
            profile_config,
            {
                "sql_id": "realdb_profiler_pkg",
                "sql_text": profile_call,
                "baseline_avg_elapsed_us": 5000000.0,
                "oracle_avg_elapsed_us": 5000000.0,
                "oracle_avg_logical_reads": 1.0,
            },
        )
    finally:
        if getattr(args, "realdb_cleanup_profile_package", False):
            cleanup_profile_test_package(config)
    if replay_row.get("ob_status") != "ok":
        return {
            "step": "plsql_profiler_smoke",
            "status": "failed",
            "ob_status": replay_row.get("ob_status"),
            "ob_error_code": replay_row.get("ob_error_code"),
            "plsql_profile_status": replay_row.get("plsql_profile_status"),
            "plsql_profile_summary": replay_row.get("plsql_profile_summary"),
            "plsql_profile_artifact_path": replay_row.get("plsql_profile_artifact_path"),
            "error": replay_row.get("ob_error_code"),
        }
    status = "passed" if replay_row.get("plsql_profile_status") == "ok" else "skipped"
    return {
        "step": "plsql_profiler_smoke",
        "status": status,
        "ob_status": replay_row.get("ob_status"),
        "ob_error_code": replay_row.get("ob_error_code"),
        "plsql_profile_status": replay_row.get("plsql_profile_status"),
        "plsql_profile_summary": replay_row.get("plsql_profile_summary"),
        "plsql_profile_artifact_path": replay_row.get("plsql_profile_artifact_path"),
        "error": replay_row.get("plsql_profile_error") or replay_row.get("ob_error_code"),
    }


def run_realdb_ob_source_smoke(config, args, run_id):
    # type: (AppConfig, argparse.Namespace, str) -> Dict[str, Any]
    smoke_config = resolve_realdb_ob_source_config(config, args)
    if not smoke_config.oceanbase_source:
        return {"step": "ob_source_capture_smoke", "status": "skipped", "reason": "ob_source_not_requested"}
    audit_available, audit_reason = probe_ob_source_audit_capability(smoke_config)
    if not audit_available:
        return {
            "step": "ob_source_capture_smoke",
            "status": "skipped",
            "reason": "source_sql_audit_unavailable",
            "error": audit_reason,
        }
    source_probe_sql = _build_realdb_probe_sql()
    for _ in range(2):
        ok, stdout, stderr = _obclient_run_sql_on_source(
            smoke_config,
            source_probe_sql,
            timeout=smoke_config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT),
            session_query_timeout_us=smoke_config.settings.get(
                "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
            ),
        )
        if not ok:
            return {
                "step": "ob_source_capture_smoke",
                "status": "failed",
                "error": stderr or stdout,
            }
    smoke_run_id = "%s_obsource" % run_id
    smoke_runtime_config = clone_app_config(
        smoke_config,
        settings_updates={
            "duration": 1,
            "interval": 1,
            "_current_run_id": smoke_run_id,
        },
    )
    workload_path = capture_workload_from_ob_source(
        smoke_runtime_config, argparse.Namespace(duration=1), smoke_run_id
    )
    replay_path = replay_workload(smoke_runtime_config, workload_path, smoke_run_id)
    report_paths = generate_report_from_replay(smoke_runtime_config, replay_path, smoke_run_id, workload_path)
    return {
        "step": "ob_source_capture_smoke",
        "status": "passed",
        "workload_path": str(workload_path),
        "replay_path": str(replay_path),
        "summary_path": str(report_paths["summary"]),
    }


def run_realdb_verification(config, args, run_id):
    # type: (AppConfig, argparse.Namespace, str) -> Path
    steps = []
    for runner in (run_realdb_oracle_smoke, run_realdb_profiler_smoke, run_realdb_ob_source_smoke):
        try:
            steps.append(runner(config, args, run_id))
        except Exception as exc:
            LOG.exception("Real DB verification step failed")
            step_name = getattr(runner, "__name__", "realdb_step")
            steps.append({"step": step_name, "status": "failed", "error": str(exc)})
    statuses = [step.get("status") for step in steps]
    overall_status = "passed"
    if any(status == "failed" for status in statuses):
        overall_status = "failed"
    elif all(status == "skipped" for status in statuses):
        overall_status = "skipped"
    summary = {
        "run_id": run_id,
        "status": overall_status,
        "generated_at": utc_now_iso(),
        "steps": steps,
    }
    summary_path = build_artifact_path(
        "realdb_verify",
        run_id,
        root_dir=config.settings["workloads_dir"],
    )
    write_json(summary_path, summary)
    return summary_path


def build_argument_parser():
    # type: () -> argparse.ArgumentParser
    parser = argparse.ArgumentParser(description="Oracle -> OceanBase performance comparator")
    parser.add_argument("--config", default="config.ini", help="Path to config.ini")
    parser.add_argument(
        "--mode",
        default=MODE_BATCH,
        choices=[
            MODE_BATCH,
            MODE_STREAM,
            MODE_REPLAY_ONLY,
            MODE_REPORT_ONLY,
            MODE_SOURCE_REPORT,
            MODE_CHECK_CONFIG,
            MODE_VERIFY_REALDB,
        ],
        help="Execution mode",
    )
    parser.add_argument("--workload", help="Input workload JSONL for replay-only mode")
    parser.add_argument("--replay", help="Input replay JSONL for report-only mode")
    parser.add_argument("--sql-file", help="Input SQL file for manual capture in batch mode")
    parser.add_argument("--wcr-path", help="Input WCR export file for Oracle workload capture")
    parser.add_argument("--duration", type=int, default=None, help="Stream mode duration in seconds")
    parser.add_argument(
        "--verify-results",
        action="store_true",
        help="Enable result-set verification for replayed SELECT statements",
    )
    parser.add_argument(
        "--result-sample-limit",
        type=int,
        default=None,
        help="Maximum row count to verify before skipping result comparison",
    )
    parser.add_argument(
        "--plsql-profile",
        action="store_true",
        help="Enable DBMS_PROFILER sampling for replayed PL/SQL statements",
    )
    parser.add_argument(
        "--plsql-profile-top-n",
        type=int,
        default=None,
        help="Maximum profiler hot lines to persist per replayed PL/SQL statement",
    )
    parser.add_argument(
        "--plsql-profile-source-context",
        type=int,
        default=None,
        help="Profiler source context lines to include around each hot line",
    )
    parser.add_argument(
        "--realdb-oracle-config",
        help="Optional reference config.ini path for Oracle-source real DB validation",
    )
    parser.add_argument(
        "--realdb-ob-source-config",
        help="Optional reference config.ini.ob path for OceanBase-source real DB validation",
    )
    parser.add_argument(
        "--realdb-deploy-profile-package",
        action="store_true",
        help="Deploy and execute a small profiler test package during verify-realdb mode",
    )
    parser.add_argument(
        "--realdb-profile-package-sql",
        help="Optional SQL file to deploy instead of the built-in profiler test package",
    )
    parser.add_argument(
        "--realdb-profile-package-call",
        help="Optional PL/SQL call text to execute for profiler validation",
    )
    parser.add_argument(
        "--realdb-cleanup-profile-package",
        action="store_true",
        help="Drop the built-in profiler test package after verify-realdb completes",
    )
    parser.add_argument("--top-n", type=int, default=None, help="Override top-N regression count")
    parser.add_argument(
        "--capture-top-n",
        type=int,
        default=None,
        help="Override Oracle capture breadth independently from report Top N",
    )
    parser.add_argument("--min-exec", type=int, default=None, help="Override minimum execution count")
    parser.add_argument("--hours", type=int, default=None, help="Override Oracle capture time window")
    parser.add_argument(
        "--timeout-factor", type=float, default=None, help="Override replay timeout factor"
    )
    parser.add_argument(
        "--slowdown-threshold",
        type=float,
        default=None,
        help="Override slowdown threshold for report highlighting",
    )
    parser.add_argument("--interval", type=int, default=None, help="Override stream poll interval")
    parser.add_argument(
        "--rolling-report-interval",
        type=int,
        default=None,
        help="Override rolling report refresh interval in seconds for source-report and stream live monitoring",
    )
    parser.add_argument(
        "--audit-poll-ms", type=int, default=None, help="Override SQL Audit poll interval in ms"
    )
    parser.add_argument(
        "--print-config-summary",
        action="store_true",
        help="Print sanitized config summary before executing",
    )
    return parser


def apply_cli_overrides(config, args):
    # type: (AppConfig, argparse.Namespace) -> None
    overrides = {
        "top_n": args.top_n,
        "capture_top_n": getattr(args, "capture_top_n", None),
        "min_exec": args.min_exec,
        "hours": args.hours,
        "duration": args.duration,
        "verify_results": True if getattr(args, "verify_results", False) else None,
        "result_sample_limit": getattr(args, "result_sample_limit", None),
        "plsql_profile": True if getattr(args, "plsql_profile", False) else None,
        "plsql_profile_top_n": getattr(args, "plsql_profile_top_n", None),
        "plsql_profile_source_context": getattr(args, "plsql_profile_source_context", None),
        "timeout_factor": args.timeout_factor,
        "slowdown_threshold": args.slowdown_threshold,
        "interval": args.interval,
        "rolling_report_interval": getattr(args, "rolling_report_interval", None),
        "audit_poll_ms": args.audit_poll_ms,
        "wcr_path": getattr(args, "wcr_path", None),
    }
    for key, value in overrides.items():
        if value is not None:
            config.settings[key] = value


def main(argv=None):
    # type: (Optional[Sequence[str]]) -> int
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    try:
        config = load_config(args.config, execution_mode=args.mode)
    except ConfigError as exc:
        configure_logging("ERROR")
        LOG.error(str(exc))
        return 2

    configure_logging(config.settings.get("log_level", "INFO"))
    apply_cli_overrides(config, args)

    if args.print_config_summary or args.mode == MODE_CHECK_CONFIG:
        print(json.dumps(summarize_config(config), ensure_ascii=True, indent=2, sort_keys=True))

    preflight = validate_runtime_paths(config)
    _print_preflight(preflight)
    if not preflight.ok:
        return 2
    emit_prominent_runtime_warnings(config, args.mode)

    run_id = generate_run_id()
    if args.mode == MODE_CHECK_CONFIG:
        LOG.info("Configuration is valid")
        return 0
    if args.mode == MODE_VERIFY_REALDB:
        try:
            summary_path = run_realdb_verification(config, args, run_id)
            LOG.info("Real DB verification complete: %s", summary_path)
            return 0
        except Exception:
            LOG.exception("Unhandled runtime error during real DB verification")
            return 1
    if args.mode == MODE_REPLAY_ONLY and not args.workload:
        LOG.error("--workload is required for replay-only mode")
        return 2
    if args.mode == MODE_REPORT_ONLY and not args.replay:
        LOG.error("--replay is required for report-only mode")
        return 2
    try:
        if args.mode == MODE_BATCH:
            workload_path = capture_workload(config, args, run_id)
            replay_path = replay_workload(config, workload_path, run_id)
            report_paths = generate_report_from_replay(config, replay_path, run_id, workload_path)
            LOG.info("Batch run complete: workload=%s replay=%s summary=%s", workload_path, replay_path, report_paths["summary"])
            return 0
        if args.mode == MODE_SOURCE_REPORT:
            if config.settings.get("source_db_mode") != SOURCE_DB_MODE_OCEANBASE:
                raise ConfigError("source-report mode requires [SETTINGS] source_db_mode = oceanbase")
            workload_path = capture_workload_from_ob_source(config, args, run_id)
            report_paths = generate_report_from_source_workload(config, workload_path, run_id)
            LOG.info("Source-report run complete: workload=%s summary=%s", workload_path, report_paths["summary"])
            return 0
        if args.mode == MODE_STREAM:
            stream_paths = run_stream_monitor_pipeline(config, args, run_id)
            LOG.info(
                "Stream run complete: workload=%s replay=%s summary=%s",
                stream_paths["workload"],
                stream_paths["replay"],
                stream_paths["summary"],
            )
            return 0
        if args.mode == MODE_REPLAY_ONLY:
            replay_path = replay_workload(config, args.workload, run_id)
            LOG.info("Replay complete: %s", replay_path)
            return 0
        if args.mode == MODE_REPORT_ONLY:
            report_paths = generate_report_from_replay(config, args.replay, run_id)
            LOG.info("Report complete: summary=%s html=%s hints=%s", report_paths["summary"], report_paths["html"], report_paths["hints"])
            return 0
        LOG.error("Mode is recognized but not implemented: %s", args.mode)
        return 2
    except ConfigError as exc:
        LOG.error(str(exc))
        return 2
    except Exception as exc:
        LOG.exception("Unhandled runtime error: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
