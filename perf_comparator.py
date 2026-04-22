#!/usr/bin/env python3
"""Single-file runtime foundation for perf_comparator."""

from __future__ import print_function

import argparse
import atexit
import configparser
import hashlib
import html
import json
import logging
import os
import re
import stat
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

try:
    import oracledb  # type: ignore
except Exception:  # pragma: no cover - optional at runtime in tests
    oracledb = None


MODE_BATCH = "batch"
MODE_STREAM = "stream"
MODE_REPLAY_ONLY = "replay-only"
MODE_REPORT_ONLY = "report-only"
MODE_CHECK_CONFIG = "check-config"
MODE_VERIFY_REALDB = "verify-realdb"

DEFAULT_WORKLOADS_DIR = "workloads"
DEFAULT_REPORT_DIR = "reports"
DEFAULT_TOP_N = 50
DEFAULT_MIN_EXEC = 5
DEFAULT_HOURS = 24
DEFAULT_TIMEOUT_FACTOR = 3.0
DEFAULT_SLOWDOWN_THRESHOLD = 0.8
DEFAULT_INTERVAL = 60
DEFAULT_AUDIT_POLL_MS = 300
DEFAULT_OBCLIENT_TIMEOUT = 120
DEFAULT_OB_SESSION_QUERY_TIMEOUT_US = 3600000000
DEFAULT_PLSQL_PROFILE_TOP_N = 10
DEFAULT_PLSQL_PROFILE_SOURCE_CONTEXT = 1

SECTION_ORACLE_SOURCE = "ORACLE_SOURCE"
SECTION_OCEANBASE_SOURCE = "OCEANBASE_SOURCE"
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


def load_config(config_path):
    # type: (str) -> AppConfig
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
    oceanbase_target = _load_ob_section(parser, SECTION_OCEANBASE_TARGET)

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
        "min_exec": _get_optional_int(settings_section, "min_exec", DEFAULT_MIN_EXEC),
        "hours": _get_optional_int(settings_section, "hours", DEFAULT_HOURS),
        "interval": _get_optional_int(settings_section, "interval", DEFAULT_INTERVAL),
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
    }

    return AppConfig(
        oracle_source=oracle_source,
        oceanbase_source=oceanbase_source,
        oceanbase_target=oceanbase_target,
        settings=settings,
        config_path=str(config_path),
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


def compute_sql_id(sql_text):
    # type: (str) -> str
    return hashlib.sha1(normalize_sql_text(sql_text).encode("utf-8")).hexdigest()[:16]


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
        sql_id = fields[2] or compute_sql_id(fields[14])
        sql_text = fields[14]
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
                "baseline_avg_logical_reads": _safe_float(fields[12]),
                "oracle_executions": 1,
                "oracle_avg_elapsed_us": _safe_float(fields[3]),
                "oracle_avg_cpu_us": None,
                "oracle_avg_logical_reads": _safe_float(fields[12]),
                "oracle_avg_physical_reads": _safe_float(fields[13]),
                "oracle_plan_hash": fields[9] or None,
                "oracle_plan_rows": [],
                "source_ob_request_id": fields[0],
                "source_ob_trace_id": fields[1],
                "source_ob_queue_time_us": _safe_float(fields[4]),
                "source_ob_get_plan_time_us": _safe_float(fields[5]),
                "source_ob_execute_time_us": _safe_float(fields[6]),
                "source_ob_net_time_us": _safe_float(fields[7]),
                "source_ob_net_wait_time_us": _safe_float(fields[8]),
                "source_ob_plan_type_raw": fields[9],
                "source_ob_plan_type": PLAN_TYPE_NAMES.get(str(fields[9]), str(fields[9])),
                "source_ob_is_hit_plan": fields[10],
                "source_ob_is_executor_rpc": fields[11],
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
    return apply_bind_literals(sql_text, bind_vars), None


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
                "hint_sql": (
                    "-- Consider table-group alignment or co-location for frequent joins\n"
                    "-- CREATE TABLEGROUP <group_name>;"
                ),
            }
        )
    if (
        net_ratio is not None
        and net_ratio > 0.6
        and ("BEGIN" in sql_text.upper() or "DECLARE" in sql_text.upper() or "PACKAGE" in sql_text.upper() or "PKG." in sql_text.upper())
        and str(row.get("ob_is_executor_rpc") or "").strip() in ("1", "True", "true", "YES", "yes")
    ):
        recommendations.append(
            {
                "rule_id": "PLSQL-RPC",
                "message": "PL/SQL or package execution appears to trigger executor RPC overhead",
                "hint_sql": (
                    "-- Review row-by-row package logic and convert to set-based SQL or batch processing\n"
                    "-- Consider FORALL or MERGE style rewrites where applicable"
                ),
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
                "hint_sql": (
                    "-- Refresh statistics, normalize SQL text, and inspect plan cache churn\n"
                    "-- Consider plan binding or outline stabilization for hot statements"
                ),
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

    if oracledb is None and config.oracle_source:
        result.warnings.append(
            "python-oracledb is not installed in the current environment; Oracle capture will not run"
        )

    return result


def summarize_config(config):
    # type: (AppConfig) -> Dict[str, Any]
    summary = {
        "config_path": config.config_path,
        "oceanbase_target": {
            "executable": config.oceanbase_target["executable"],
            "host": config.oceanbase_target["host"],
            "port": config.oceanbase_target["port"],
            "user_string": config.oceanbase_target["user_string"],
        },
        "settings": config.settings,
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
        "wcr": False,
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
    ob_cfg = config.oceanbase_target
    capabilities = {
        "obclient": Path(ob_cfg["executable"]).exists(),
        "sql_audit": False,
        "explain": False,
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
            "obclient_executable": config.oceanbase_target["executable"],
            "obclient_host": config.oceanbase_target["host"],
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
    binds.update({"hours": int(config.settings["hours"]), "min_exec": int(config.settings["min_exec"]), "top_n": int(config.settings["top_n"])})
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
    binds.update({"hours": int(config.settings["hours"]), "min_exec": int(config.settings["min_exec"]), "top_n": int(config.settings["top_n"])})
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


def _build_source_ob_audit_query(last_request_id):
    # type: (int) -> str
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
          SQL_TEXT
        FROM GV$OB_SQL_AUDIT
        WHERE REQUEST_ID > {last_request_id}
        ORDER BY REQUEST_ID
    """.format(last_request_id=int(last_request_id))


def _obclient_run_sql_on_source(config, sql_text, timeout=None, session_query_timeout_us=0):
    # type: (AppConfig, str, Optional[int], int) -> Tuple[bool, str, str]
    return obclient_run_sql(
        config.oceanbase_source,
        sql_text,
        timeout=timeout,
        session_query_timeout_us=session_query_timeout_us,
    )


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
    last_request_id = 0
    default_schema = config.settings["source_schemas"][0]
    while True:
        ok, stdout, stderr = _obclient_run_sql_on_source(
            config,
            _build_source_ob_audit_query(last_request_id),
            timeout=config.settings.get("obclient_timeout", DEFAULT_OBCLIENT_TIMEOUT),
            session_query_timeout_us=config.settings.get(
                "ob_session_query_timeout_us", DEFAULT_OB_SESSION_QUERY_TIMEOUT_US
            ),
        )
        if not ok:
            if time.time() - started >= duration:
                break
            LOG.warning("OceanBase source audit polling failed: %s", stderr or stdout)
            time.sleep(poll_interval)
            continue
        rows = parse_ob_audit_rows(stdout, default_schema, captured_at=utc_now_iso())
        if rows:
            append_jsonl(workload_path, rows)
            last_request_id = max(int(row.get("source_ob_request_id") or 0) for row in rows)
        if time.time() - started >= duration:
            break
        time.sleep(poll_interval)
    if not workload_path.exists():
        raise ConfigError("OceanBase source capture did not produce any workload rows")
    return workload_path


def capture_workload(config, args, run_id):
    # type: (AppConfig, argparse.Namespace, str) -> Path
    if config.settings.get("source_db_mode") == SOURCE_DB_MODE_OCEANBASE:
        return capture_workload_from_ob_source(config, args, run_id)
    capture_capabilities = probe_oracle_capabilities(config)
    capture_capabilities["run_id"] = run_id
    capture_capabilities["oracle_dsn"] = config.oracle_source["dsn"]
    replay_capabilities = probe_replay_capabilities(config)
    replay_capabilities["run_id"] = run_id
    write_capability_files(config, run_id, capture_capabilities, replay_capabilities)
    if getattr(args, "sql_file", None):
        return capture_from_sql_file(config, args.sql_file, run_id)
    if capture_capabilities.get("awr"):
        preferred_source = "awr"
    elif capture_capabilities.get("vsql"):
        preferred_source = "vsql"
    else:
        raise ConfigError("Oracle capture is unavailable and --sql-file was not provided")
    connection = _open_oracle_connection(config)
    try:
        if preferred_source == "awr":
            rows = _capture_from_awr(connection, config)
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


def _escape_sql_string(value):
    # type: (str) -> str
    return str(value or "").replace("'", "''")


def _build_plsql_profiler_payload(rendered_sql, profiler_comment):
    # type: (str, str) -> str
    return "\n".join(
        [
            "CALL DBMS_PROFILER.START_PROFILER('%s');" % _escape_sql_string(profiler_comment),
            str(rendered_sql or "").rstrip().rstrip(";") + ";",
            "CALL DBMS_PROFILER.STOP_PROFILER();",
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


def _fetch_plsql_source_context(config, owner, unit_name, unit_type, line_no, context_size):
    # type: (AppConfig, str, str, str, int, int) -> List[Dict[str, Any]]
    low_line = max(1, int(line_no) - max(0, int(context_size or 0)))
    high_line = int(line_no) + max(0, int(context_size or 0))
    query = """
        SELECT LINE, TEXT
        FROM ALL_SOURCE
        WHERE OWNER = '{owner}'
          AND NAME = '{unit_name}'
          AND TYPE = '{unit_type}'
          AND LINE BETWEEN {low_line} AND {high_line}
        ORDER BY LINE
    """.format(
        owner=_escape_sql_string(owner),
        unit_name=_escape_sql_string(unit_name),
        unit_type=_escape_sql_string(unit_type),
        low_line=low_line,
        high_line=high_line,
    )
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
    context_rows = []
    for raw_line in stdout.splitlines():
        fields = raw_line.split("\t", 1)
        if len(fields) < 2:
            continue
        line_value = _safe_int(fields[0])
        if line_value is None:
            continue
        context_rows.append({"line": line_value, "text": fields[1]})
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
            d.TOTAL_TIME,
            s.TEXT
          FROM PLSQL_PROFILER_UNITS u
          JOIN PLSQL_PROFILER_DATA d
            ON d.RUNID = u.RUNID
           AND d.UNIT_NUMBER = u.UNIT_NUMBER
          LEFT JOIN ALL_SOURCE s
            ON s.OWNER = u.UNIT_OWNER
           AND s.NAME = u.UNIT_NAME
           AND s.TYPE = u.UNIT_TYPE
           AND s.LINE = d.LINE#
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
        if len(fields) < 7:
            continue
        line_no = _safe_int(fields[3])
        if line_no is None:
            continue
        source_text = fields[6]
        row = {
            "owner": fields[0],
            "unit_name": fields[1],
            "unit_type": fields[2],
            "line": line_no,
            "total_occur": _safe_int(fields[4]),
            "total_time_us": _safe_float(fields[5]),
            "source_text": source_text,
        }
        if int(context_size or 0) > 0:
            row["context_lines"] = _fetch_plsql_source_context(
                config,
                fields[0],
                fields[1],
                fields[2],
                line_no,
                int(context_size or 0),
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


def collect_plsql_profile(config, workload_row, rendered_sql, timeout_seconds):
    # type: (AppConfig, Dict[str, Any], str, int) -> Dict[str, Any]
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
            "captured_at": utc_now_iso(),
        },
    )
    return {
        "status": "ok",
        "error": "",
        "runid": runid,
        "top_lines": top_lines,
        "artifact_path": str(artifact_path),
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
        merged["plsql_profile_summary"] = summarize_plsql_profile(
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
        base["recommendations"] = build_recommendations(
            base,
            slowdown_threshold=float(config.settings.get("slowdown_threshold", DEFAULT_SLOWDOWN_THRESHOLD)),
        )
        enriched_rows.append(base)
    sort_key = lambda item: (
        item.get("speedup_ratio") is None,
        item.get("speedup_ratio") if item.get("speedup_ratio") is not None else 999999.0,
    )
    enriched_rows = sorted(enriched_rows, key=sort_key)
    top_n = int(config.settings.get("top_n", DEFAULT_TOP_N))
    selected_rows = enriched_rows[:top_n]
    report_dir = Path(config.settings["report_dir"])
    summary_path = build_artifact_path("report_summary", run_id, root_dir=report_dir)
    html_path = build_artifact_path("report_html", run_id, root_dir=report_dir)
    hints_path = build_artifact_path("report_hints", run_id, root_dir=report_dir)

    summary_lines = [
        "Run ID: %s" % run_id,
        "Replay file: %s" % str(replay_path),
        "Total statements: %d" % len(enriched_rows),
        "Successful statements: %d" % sum(1 for row in enriched_rows if row.get("ob_status") == "ok"),
        "Failed statements: %d" % sum(1 for row in enriched_rows if row.get("ob_status") != "ok"),
        "Verified matches: %d" % sum(1 for row in enriched_rows if row.get("verification_status") == "match"),
        "Verified mismatches: %d" % sum(1 for row in enriched_rows if row.get("verification_status") == "mismatch"),
        "Verified skipped: %d" % sum(1 for row in enriched_rows if row.get("verification_status") == "skipped"),
        "",
        "Top regressions:",
    ]
    for idx, row in enumerate(selected_rows, 1):
        rule_ids = ",".join(item["rule_id"] for item in row.get("recommendations", [])) or "none"
        verification_status = summarize_verification_evidence(row)
        plan_monitor_summary = summarize_plan_monitor_evidence(row)
        plan_risk_summary = summarize_plan_diff_signals(row)
        plsql_profile_summary = summarize_plsql_profile(row)
        summary_lines.append(
            "%d. sql_id=%s speedup_ratio=%s baseline_us=%s ob_us=%s rules=%s verification=%s monitor=%s plan_risk=%s plsql=%s"
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
            )
        )
    write_text(summary_path, "\n".join(summary_lines) + "\n")

    html_rows = []
    for row in selected_rows:
        verification_status = summarize_verification_evidence(row)
        plan_monitor_summary = summarize_plan_monitor_evidence(row)
        plan_risk_summary = summarize_plan_diff_signals(row)
        plsql_profile_summary = summarize_plsql_profile(row)
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
                evidence=html.escape(
                    "verification=%s | monitor=%s | plan-risk=%s | plsql=%s"
                    % (
                        verification_status,
                        plan_monitor_summary,
                        plan_risk_summary,
                        plsql_profile_summary,
                    )
                ),
                sql=html.escape(str(row.get("sql_text") or "")),
            )
        )
    html_content = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>perf_comparator report</title>
<style>
body { font-family: Arial, sans-serif; margin: 24px; }
table { border-collapse: collapse; width: 100%%; }
th, td { border: 1px solid #ddd; padding: 8px; vertical-align: top; }
th { background: #f4f4f4; text-align: left; }
pre { white-space: pre-wrap; margin: 0; }
</style></head><body>
<h1>perf_comparator report</h1>
<p>Run ID: %s</p>
<table>
<thead><tr><th>SQL ID</th><th>Speedup Ratio</th><th>Baseline Avg (us)</th><th>OB Elapsed (us)</th><th>Rules</th><th>Evidence</th><th>SQL</th></tr></thead>
<tbody>%s</tbody></table></body></html>
""" % (html.escape(run_id), "".join(html_rows))
    write_text(html_path, html_content)

    hints_lines = ["-- perf_comparator recommendations", "-- run_id: %s" % run_id, ""]
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
    oceanbase_target=None,
    settings_updates=None,
    config_path=None,
):
    # type: (AppConfig, Optional[Dict[str, str]], Optional[Dict[str, str]], Optional[Dict[str, str]], Optional[Dict[str, Any]], Optional[str]) -> AppConfig
    settings = dict(config.settings)
    if settings_updates:
        settings.update(settings_updates)
    return AppConfig(
        oracle_source=dict(oracle_source if oracle_source is not None else config.oracle_source),
        oceanbase_source=dict(oceanbase_source if oceanbase_source is not None else config.oceanbase_source),
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
            MODE_CHECK_CONFIG,
            MODE_VERIFY_REALDB,
        ],
        help="Execution mode",
    )
    parser.add_argument("--workload", help="Input workload JSONL for replay-only mode")
    parser.add_argument("--replay", help="Input replay JSONL for report-only mode")
    parser.add_argument("--sql-file", help="Input SQL file for manual capture in batch mode")
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
        "audit_poll_ms": args.audit_poll_ms,
    }
    for key, value in overrides.items():
        if value is not None:
            config.settings[key] = value


def main(argv=None):
    # type: (Optional[Sequence[str]]) -> int
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    try:
        config = load_config(args.config)
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
        if args.mode == MODE_STREAM:
            workload_path = stream_capture_workload(config, args, run_id)
            replay_path = replay_workload(config, workload_path, run_id)
            report_paths = generate_report_from_replay(config, replay_path, run_id, workload_path)
            LOG.info("Stream run complete: workload=%s replay=%s summary=%s", workload_path, replay_path, report_paths["summary"])
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
