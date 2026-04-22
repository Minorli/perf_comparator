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
    "report_html": ("perf_report", ".html"),
    "report_summary": ("perf_report", "_summary.txt"),
    "report_hints": ("perf_hints", ".sql"),
}

LOG = logging.getLogger("perf_comparator")
_SECURE_FILES = set()  # type: ignore[var-annotated]
_SECURE_FILES_LOCK = threading.Lock()


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

    oracle_section = _get_required_section(parser, SECTION_ORACLE_SOURCE)
    settings_section = _get_required_section(parser, SECTION_SETTINGS)
    source_db_mode = (settings_section.get("source_db_mode") or SOURCE_DB_MODE_ORACLE).strip().lower()
    if source_db_mode not in (SOURCE_DB_MODE_ORACLE, SOURCE_DB_MODE_OCEANBASE):
        raise ConfigError("[SETTINGS] source_db_mode must be oracle or oceanbase")

    oracle_source = {
        "user": _get_required_value(oracle_section, SECTION_ORACLE_SOURCE, "user"),
        "password": _get_required_value(oracle_section, SECTION_ORACLE_SOURCE, "password"),
        "dsn": _get_required_value(oracle_section, SECTION_ORACLE_SOURCE, "dsn"),
    }
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
                "source_ob_is_hit_plan": fields[10],
                "source_ob_is_executor_rpc": fields[11],
            }
        )
    return rows


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
    speedup_ratio = None
    if oracle_elapsed > 0.0 and ob_elapsed > 0.0:
        speedup_ratio = oracle_elapsed / ob_elapsed
    read_amplification = None
    if oracle_reads > 0.0 and ob_reads >= 0.0:
        read_amplification = ob_reads / oracle_reads
    net_ratio = None
    if ob_elapsed > 0.0:
        net_ratio = ob_net / ob_elapsed
    return {
        "speedup_ratio": speedup_ratio,
        "read_amplification": read_amplification,
        "net_ratio": net_ratio,
        "plan_changed": bool(
            row.get("oracle_plan_hash")
            and row.get("ob_plan_hash")
            and str(row.get("oracle_plan_hash")) != str(row.get("ob_plan_hash"))
        ),
    }


def build_recommendations(row, slowdown_threshold):
    # type: (Dict[str, Any], float) -> List[Dict[str, str]]
    recommendations = []
    speedup_ratio = row.get("speedup_ratio")
    net_ratio = row.get("net_ratio")
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

    if oracledb is None:
        result.warnings.append(
            "python-oracledb is not installed in the current environment; Oracle capture will not run"
        )

    return result


def summarize_config(config):
    # type: (AppConfig) -> Dict[str, Any]
    host, port, service_name = parse_oracle_dsn(config.oracle_source["dsn"])
    summary = {
        "config_path": config.config_path,
        "oracle_source": {
            "user": config.oracle_source["user"],
            "host": host,
            "port": port,
            "service_name": service_name,
        },
        "oceanbase_target": {
            "executable": config.oceanbase_target["executable"],
            "host": config.oceanbase_target["host"],
            "port": config.oceanbase_target["port"],
            "user_string": config.oceanbase_target["user_string"],
        },
        "settings": config.settings,
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
        "awr": False,
        "vsql": False,
        "unified_audit": False,
        "wcr": False,
        "sql_file": True,
    }
    if oracledb is None:
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
    write_json(
        build_artifact_path("capture_capability", run_id, root_dir=workloads_dir),
        capture_capabilities
        or {
            "run_id": run_id,
            "oracle_driver_available": bool(oracledb is not None),
            "oracle_dsn": config.oracle_source["dsn"],
        },
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


def _safe_float(value):
    # type: (Any) -> Optional[float]
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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
    audit_row = {}
    if audit_collector is not None:
        audit_collector.collect_once()
        audit_row = audit_collector.match_for_sql(rendered_sql)
    if not audit_row:
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
        "ob_plan_error": explain_stderr if not explain_ok else "",
        "ob_plan_hash": compute_sql_id(explain_stdout) if explain_ok and explain_stdout else None,
        "replayed_at": started_at,
        "ob_stdout_preview": (stdout or "")[:500],
    }
    merged = dict(workload_row)
    merged.update(replay_row)
    merged.update(derive_replay_metrics(merged))
    merged["recommendations"] = build_recommendations(
        merged, slowdown_threshold=float(config.settings.get("slowdown_threshold", DEFAULT_SLOWDOWN_THRESHOLD))
    )
    return merged


def replay_workload(config, workload_path, run_id):
    # type: (AppConfig, Union[str, Path], str) -> Path
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
        "",
        "Top regressions:",
    ]
    for idx, row in enumerate(selected_rows, 1):
        rule_ids = ",".join(item["rule_id"] for item in row.get("recommendations", [])) or "none"
        summary_lines.append(
            "%d. sql_id=%s speedup_ratio=%s baseline_us=%s ob_us=%s rules=%s"
            % (
                idx,
                row.get("sql_id"),
                _format_ratio(row.get("speedup_ratio")),
                row.get("baseline_avg_elapsed_us") or row.get("oracle_avg_elapsed_us"),
                row.get("ob_elapsed_us"),
                rule_ids,
            )
        )
    write_text(summary_path, "\n".join(summary_lines) + "\n")

    html_rows = []
    for row in selected_rows:
        html_rows.append(
            "<tr><td>{sql_id}</td><td>{speedup}</td><td>{oracle}</td><td>{ob}</td><td>{rules}</td><td><pre>{sql}</pre></td></tr>".format(
                sql_id=html.escape(str(row.get("sql_id"))),
                speedup=html.escape(_format_ratio(row.get("speedup_ratio"))),
                oracle=html.escape(str(row.get("baseline_avg_elapsed_us") or row.get("oracle_avg_elapsed_us"))),
                ob=html.escape(str(row.get("ob_elapsed_us"))),
                rules=html.escape(",".join(item["rule_id"] for item in row.get("recommendations", [])) or "none"),
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
<thead><tr><th>SQL ID</th><th>Speedup Ratio</th><th>Baseline Avg (us)</th><th>OB Elapsed (us)</th><th>Rules</th><th>SQL</th></tr></thead>
<tbody>%s</tbody></table></body></html>
""" % (html.escape(run_id), "".join(html_rows))
    write_text(html_path, html_content)

    hints_lines = ["-- perf_comparator recommendations", "-- run_id: %s" % run_id, ""]
    for row in selected_rows:
        hints_lines.append("-- sql_id: %s" % row.get("sql_id"))
        for item in row.get("recommendations", []):
            hints_lines.append("-- %s: %s" % (item["rule_id"], item["message"]))
            hints_lines.append(item["hint_sql"])
        hints_lines.append("")
    write_text(hints_path, "\n".join(hints_lines).rstrip() + "\n")
    return {"summary": summary_path, "html": html_path, "hints": hints_path}


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
        ],
        help="Execution mode",
    )
    parser.add_argument("--workload", help="Input workload JSONL for replay-only mode")
    parser.add_argument("--replay", help="Input replay JSONL for report-only mode")
    parser.add_argument("--sql-file", help="Input SQL file for manual capture in batch mode")
    parser.add_argument("--duration", type=int, default=None, help="Stream mode duration in seconds")
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
