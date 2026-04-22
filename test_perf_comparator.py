import argparse
import io
import os
import stat
import tempfile
import textwrap
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

import perf_comparator


class PerfComparatorConfigTests(unittest.TestCase):
    def test_load_config_uses_comparator_style_sections(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.ini"
            config_path.write_text(
                textwrap.dedent(
                    """
                    [ORACLE_SOURCE]
                    user = scott
                    password = tiger
                    dsn = 127.0.0.1:1521/ORCL

                    [OCEANBASE_TARGET]
                    executable = /bin/echo
                    host = 127.0.0.1
                    port = 2881
                    user_string = root@test#obcluster
                    password = secret

                    [SETTINGS]
                    source_schemas = APP,APP_AUDIT
                    ob_session_query_timeout_us = 3600000
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            cfg = perf_comparator.load_config(str(config_path))

            self.assertEqual(cfg.oracle_source["user"], "scott")
            self.assertEqual(cfg.oracle_source["dsn"], "127.0.0.1:1521/ORCL")
            self.assertEqual(cfg.oceanbase_target["user_string"], "root@test#obcluster")
            self.assertEqual(cfg.settings["source_schemas"], ["APP", "APP_AUDIT"])
            self.assertEqual(cfg.settings["ob_session_query_timeout_us"], 3600000)

    def test_load_config_requires_sections_and_keys(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.ini"
            config_path.write_text(
                textwrap.dedent(
                    """
                    [ORACLE_SOURCE]
                    user = scott
                    password = tiger
                    dsn = 127.0.0.1:1521/ORCL

                    [SETTINGS]
                    source_schemas = APP
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            with self.assertRaises(perf_comparator.ConfigError) as ctx:
                perf_comparator.load_config(str(config_path))

            self.assertIn("OCEANBASE_TARGET", str(ctx.exception))

    def test_load_config_requires_ob_source_in_oceanbase_source_mode(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.ini"
            config_path.write_text(
                textwrap.dedent(
                    """
                    [ORACLE_SOURCE]
                    user = scott
                    password = tiger
                    dsn = 127.0.0.1:1521/ORCL

                    [OCEANBASE_TARGET]
                    executable = /bin/echo
                    host = 127.0.0.1
                    port = 2881
                    user_string = root@test#obcluster
                    password = secret

                    [SETTINGS]
                    source_db_mode = oceanbase
                    source_schemas = APP
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            with self.assertRaises(perf_comparator.ConfigError) as ctx:
                perf_comparator.load_config(str(config_path))

            self.assertIn("OCEANBASE_SOURCE", str(ctx.exception))

    def test_load_config_parses_ob_source_section(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.ini"
            config_path.write_text(
                textwrap.dedent(
                    """
                    [ORACLE_SOURCE]
                    user = scott
                    password = tiger
                    dsn = 127.0.0.1:1521/ORCL

                    [OCEANBASE_SOURCE]
                    executable = /bin/echo
                    host = 127.0.0.2
                    port = 2883
                    user_string = app@test#obcluster
                    password = source_secret

                    [OCEANBASE_TARGET]
                    executable = /bin/echo
                    host = 127.0.0.1
                    port = 2881
                    user_string = root@test#obcluster
                    password = target_secret

                    [SETTINGS]
                    source_db_mode = oceanbase
                    source_schemas = APP
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            cfg = perf_comparator.load_config(str(config_path))

            self.assertEqual(cfg.settings["source_db_mode"], "oceanbase")
            self.assertEqual(cfg.oceanbase_source["host"], "127.0.0.2")
            self.assertEqual(cfg.oceanbase_source["user_string"], "app@test#obcluster")


class PerfComparatorUtilityTests(unittest.TestCase):
    def test_parse_oracle_dsn(self):
        host, port, service = perf_comparator.parse_oracle_dsn("db.example.com:1521/ORCL")
        self.assertEqual((host, port, service), ("db.example.com", "1521", "ORCL"))

    def test_parse_oracle_dsn_rejects_invalid_format(self):
        with self.assertRaises(perf_comparator.ConfigError):
            perf_comparator.parse_oracle_dsn("db.example.com")

    def test_build_artifact_path_uses_timestamped_names(self):
        path = perf_comparator.build_artifact_path("workload", "20260422_153000", root_dir="workloads")
        self.assertEqual(path, Path("workloads") / "workload_20260422_153000.jsonl")

    def test_append_jsonl_writes_multiple_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "workload_20260422_153000.jsonl"
            perf_comparator.append_jsonl(path, {"sql_id": "a"})
            perf_comparator.append_jsonl(path, [{"sql_id": "b"}, {"sql_id": "c"}])
            lines = path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 3)
            self.assertIn('"sql_id": "a"', lines[0])

    def test_split_sql_text_handles_semicolons_and_dollar_delimiter(self):
        sql_text = textwrap.dedent(
            """
            SELECT * FROM orders WHERE status = 'A;B';
            $$
            BEGIN
              NULL;
            END;
            $$
            SELECT 1 FROM dual;
            """
        )
        statements = perf_comparator.split_sql_text(sql_text)
        self.assertEqual(len(statements), 3)
        self.assertIn("A;B", statements[0])
        self.assertTrue(statements[1].startswith("BEGIN"))
        self.assertEqual(statements[2], "SELECT 1 FROM dual")

    def test_apply_bind_literals_replaces_positional_bind_values(self):
        rendered = perf_comparator.apply_bind_literals(
            "SELECT * FROM orders WHERE status = :1 AND id = :B2",
            {"1": "ACTIVE", "2": 42},
        )
        self.assertIn("'ACTIVE'", rendered)
        self.assertIn("42", rendered)

    def test_render_sql_for_replay_returns_skip_for_unsupported_bind_types(self):
        rendered, skip_reason = perf_comparator.render_sql_for_replay(
            "SELECT * FROM orders WHERE payload = :1",
            {"1": {"complex": "value"}},
        )
        self.assertIsNone(rendered)
        self.assertIn("unsupported bind types", skip_reason)

    def test_derive_replay_metrics_calculates_speedup_ratio(self):
        row = {
            "oracle_avg_elapsed_us": 400.0,
            "ob_elapsed_us": 800.0,
            "oracle_avg_logical_reads": 10.0,
            "ob_logical_reads": 50.0,
            "ob_net_time_us": 200.0,
            "ob_plan_hash": "abc",
            "oracle_plan_hash": "def",
        }
        metrics = perf_comparator.derive_replay_metrics(row)
        self.assertAlmostEqual(metrics["speedup_ratio"], 0.5)
        self.assertAlmostEqual(metrics["read_amplification"], 5.0)
        self.assertAlmostEqual(metrics["net_ratio"], 0.25)
        self.assertTrue(metrics["plan_changed"])

    def test_build_recommendations_marks_slow_regression(self):
        row = {
            "sql_id": "sql-1",
            "speedup_ratio": 0.5,
            "ob_elapsed_us": 8000,
            "oracle_avg_elapsed_us": 4000,
            "net_ratio": 0.7,
            "plan_changed": True,
            "ob_status": "ok",
        }
        recommendations = perf_comparator.build_recommendations(row, slowdown_threshold=0.8)
        self.assertTrue(any(item["rule_id"] == "DIST-JOIN" for item in recommendations))
        self.assertTrue(any(item["rule_id"] == "PLAN-CHANGED" for item in recommendations))

    def test_build_recommendations_handles_skip_status(self):
        row = {"ob_status": "skip", "speedup_ratio": None, "net_ratio": None}
        recommendations = perf_comparator.build_recommendations(row, slowdown_threshold=0.8)
        self.assertEqual(recommendations[0]["rule_id"], "REPLAY-SKIP")

    def test_parse_ob_audit_rows_into_workload_events(self):
        stdout = (
            "101\ttrace-1\tsql-1\t1200\t30\t20\t1150\t800\t20\t3\t1\t1\t90\t4\tSELECT * FROM orders\n"
            "102\ttrace-2\tsql-2\t600\t10\t10\t580\t100\t5\t1\t1\t0\t20\t1\tBEGIN pkg.run(); END"
        )
        rows = perf_comparator.parse_ob_audit_rows(stdout, "APP", captured_at="2026-04-22T15:00:00Z")
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["sql_id"], "sql-1")
        self.assertEqual(rows[0]["source"], "ob_sql_audit")
        self.assertEqual(rows[0]["baseline_source_mode"], "oceanbase")
        self.assertEqual(rows[0]["oracle_avg_elapsed_us"], 1200.0)
        self.assertEqual(rows[1]["sql_text"], "BEGIN pkg.run(); END")

    def test_parse_explain_plan_text_extracts_plan_rows(self):
        plan_text = textwrap.dedent(
            """
            ==============================================
            |ID|OPERATOR              |NAME             |
            ----------------------------------------------
            |0 |PX COORDINATOR        |                 |
            |1 | HASH JOIN            |                 |
            |2 |  TABLE SCAN          |ORDERS           |
            |3 |  TABLE LOOKUP        |ORDER_ITEMS      |
            ==============================================
            """
        )
        rows = perf_comparator.parse_explain_plan_text(plan_text)
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[0]["id"], 0)
        self.assertEqual(rows[2]["operator"], "TABLE SCAN")
        self.assertEqual(rows[3]["name"], "ORDER_ITEMS")

    def test_parse_plan_monitor_rows_extracts_operator_metrics(self):
        stdout = (
            "1\tHASH JOIN\t1000\t1024\t2048\t0\n"
            "2\tTABLE SCAN\t500\t256\t512\t64"
        )
        rows = perf_comparator.parse_plan_monitor_rows(stdout)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["plan_line_id"], 1)
        self.assertEqual(rows[0]["operator"], "HASH JOIN")
        self.assertEqual(rows[1]["workarea_tempseg"], 64.0)

    def test_verify_result_sets_match(self):
        result = perf_comparator.verify_result_sets(
            source_rows=[("A", 1), ("B", 2)],
            target_rows=[("B", 2), ("A", 1)],
            sample_limit=10,
        )
        self.assertEqual(result["status"], "match")
        self.assertTrue(result["source_hash"])
        self.assertEqual(result["mismatch_sample"], [])

    def test_verify_result_sets_detects_mismatch(self):
        result = perf_comparator.verify_result_sets(
            source_rows=[("A", 1), ("B", 2)],
            target_rows=[("A", 1), ("C", 3)],
            sample_limit=10,
        )
        self.assertEqual(result["status"], "mismatch")
        self.assertTrue(result["mismatch_sample"])

    def test_verify_result_sets_skips_large_result(self):
        source_rows = [(idx,) for idx in range(11)]
        target_rows = [(idx,) for idx in range(11)]
        result = perf_comparator.verify_result_sets(
            source_rows=source_rows,
            target_rows=target_rows,
            sample_limit=10,
        )
        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "too_large")

    def test_build_recommendations_handles_plan_miss_lock_hot_and_plsql(self):
        row = {
            "sql_text": "BEGIN pkg.run(); END",
            "ob_status": "ok",
            "speedup_ratio": 0.4,
            "net_ratio": 0.7,
            "plan_changed": False,
            "ob_is_executor_rpc": "1",
            "ob_queue_time_us": 900.0,
            "ob_execute_time_us": 200.0,
            "ob_retry_cnt": 2,
            "ob_is_hit_plan": "0",
            "ob_get_plan_time_us": 300.0,
            "ob_elapsed_us": 1000.0,
            "ob_memstore_read_rows": 800.0,
            "ob_ssstore_read_rows": 100.0,
            "ob_bloom_filter_filtered": 0.0,
        }
        recommendations = perf_comparator.build_recommendations(row, slowdown_threshold=0.8)
        rule_ids = [item["rule_id"] for item in recommendations]
        self.assertIn("DIST-JOIN", rule_ids)
        self.assertIn("PLSQL-RPC", rule_ids)
        self.assertIn("LOCK-HOT", rule_ids)
        self.assertIn("PLAN-MISS", rule_ids)
        self.assertIn("LSM-JITTER", rule_ids)


class PerfComparatorObclientTests(unittest.TestCase):
    def test_build_obclient_command_args_hides_password(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ob_cfg = {
                "executable": "/bin/echo",
                "host": "127.0.0.1",
                "port": "2881",
                "user_string": "root@test#obcluster",
                "password": 's"ec\\ret',
                "__temp_dir": tmpdir,
            }

            args = perf_comparator.build_obclient_command_args(ob_cfg, extra_args=["-ss"])

            self.assertEqual(args[:7], ["/bin/echo", args[1], "-h", "127.0.0.1", "-P", "2881", "-u"])
            self.assertEqual(args[7], "root@test#obcluster")
            self.assertEqual(args[-1], "-ss")
            self.assertNotIn('s"ec\\ret', " ".join(args))

            defaults_arg = args[1]
            self.assertTrue(defaults_arg.startswith("--defaults-extra-file="))
            defaults_path = Path(defaults_arg.split("=", 1)[1])
            self.assertTrue(defaults_path.exists())
            mode = stat.S_IMODE(os.stat(str(defaults_path)).st_mode)
            self.assertEqual(mode, 0o600)
            contents = defaults_path.read_text(encoding="utf-8")
            self.assertIn('[client]', contents)
            self.assertIn('password="s\\"ec\\\\ret"', contents)

            perf_comparator.cleanup_secure_tempfiles()
            self.assertFalse(defaults_path.exists())

    def test_validate_runtime_paths_reports_missing_obclient(self):
        cfg = perf_comparator.AppConfig(
            oracle_source={"user": "u", "password": "p", "dsn": "127.0.0.1:1521/ORCL"},
            oceanbase_source={},
            oceanbase_target={
                "executable": "/path/does/not/exist/obclient",
                "host": "127.0.0.1",
                "port": "2881",
                "user_string": "root@test#obcluster",
                "password": "secret",
            },
            settings={"ob_session_query_timeout_us": 0, "source_schemas": ["APP"]},
            config_path="config.ini",
        )

        result = perf_comparator.validate_runtime_paths(cfg)

        self.assertFalse(result.ok)
        self.assertTrue(any("obclient" in item for item in result.errors))

    def test_validate_runtime_paths_checks_ob_source_when_enabled(self):
        cfg = perf_comparator.AppConfig(
            oracle_source={"user": "u", "password": "p", "dsn": "127.0.0.1:1521/ORCL"},
            oceanbase_source={
                "executable": "/path/does/not/exist/source-obclient",
                "host": "127.0.0.2",
                "port": "2883",
                "user_string": "app@test#obcluster",
                "password": "secret",
            },
            oceanbase_target={
                "executable": "/bin/echo",
                "host": "127.0.0.1",
                "port": "2881",
                "user_string": "root@test#obcluster",
                "password": "secret",
            },
            settings={"source_db_mode": "oceanbase", "ob_session_query_timeout_us": 0, "source_schemas": ["APP"]},
            config_path="config.ini",
        )

        result = perf_comparator.validate_runtime_paths(cfg)

        self.assertFalse(result.ok)
        self.assertTrue(any("source obclient" in item.lower() for item in result.errors))


class PerfComparatorReplayEvidenceTests(unittest.TestCase):
    def _build_config(self, verify_results=False):
        return perf_comparator.AppConfig(
            oracle_source={"user": "u", "password": "p", "dsn": "127.0.0.1:1521/ORCL"},
            oceanbase_source={},
            oceanbase_target={
                "executable": "/bin/echo",
                "host": "127.0.0.1",
                "port": "2881",
                "user_string": "root@test#obcluster",
                "password": "secret",
            },
            settings={
                "source_schemas": ["APP"],
                "verify_results": verify_results,
                "result_sample_limit": 100,
                "slowdown_threshold": 0.8,
                "obclient_timeout": 5,
                "ob_session_query_timeout_us": 0,
                "timeout_factor": 3.0,
                "workloads_dir": ".",
            },
            config_path="config.ini",
        )

    def test_replay_statement_attaches_verification_and_plan_monitor(self):
        config = self._build_config(verify_results=True)
        workload_row = {
            "sql_id": "sql-1",
            "sql_text": "SELECT * FROM orders",
            "baseline_avg_elapsed_us": 1000.0,
            "oracle_avg_elapsed_us": 1000.0,
            "oracle_avg_logical_reads": 20.0,
        }
        backend = mock.Mock()
        backend.execute.return_value = (True, "ok", "")
        backend.explain.return_value = (
            True,
            "|0 |PX COORDINATOR|\n|1 |HASH JOIN|",
            "",
        )

        with mock.patch.object(
            perf_comparator,
            "_query_recent_audit_row",
            return_value={
                "ob_elapsed_us": 3000.0,
                "ob_net_time_us": 2100.0,
                "ob_plan_type_raw": "3",
                "ob_is_hit_plan": "1",
                "ob_is_executor_rpc": "0",
                "ob_logical_reads": 60.0,
                "trace_id": "trace-1",
            },
        ), mock.patch.object(
            perf_comparator,
            "perform_result_verification",
            return_value={
                "status": "mismatch",
                "reason": "",
                "source_hash": "src-hash",
                "target_hash": "tgt-hash",
                "artifact_path": "/tmp/mismatch.jsonl",
                "mismatch_sample": [{"source": "A", "target": "B"}],
            },
        ) as verify_mock, mock.patch.object(
            perf_comparator,
            "collect_plan_monitor_rows",
            return_value=[
                {
                    "plan_line_id": 1,
                    "operator": "HASH JOIN",
                    "output_rows": 1000.0,
                    "workarea_mem": 1024.0,
                    "workarea_max_mem": 2048.0,
                    "workarea_tempseg": 64.0,
                }
            ],
        ) as monitor_mock:
            replay_row = perf_comparator.replay_statement(config, workload_row, backend=backend)

        verify_mock.assert_called_once()
        monitor_mock.assert_called_once()
        self.assertEqual(replay_row["verification_status"], "mismatch")
        self.assertEqual(replay_row["verification_artifact_path"], "/tmp/mismatch.jsonl")
        self.assertEqual(replay_row["verification_source_hash"], "src-hash")
        self.assertEqual(replay_row["plan_monitor_rows"][0]["operator"], "HASH JOIN")

    def test_replay_statement_skips_plan_monitor_without_evidence_gate(self):
        config = self._build_config(verify_results=False)
        workload_row = {
            "sql_id": "sql-1",
            "sql_text": "SELECT * FROM orders",
            "baseline_avg_elapsed_us": 3000.0,
            "oracle_avg_elapsed_us": 3000.0,
            "oracle_avg_logical_reads": 20.0,
        }
        backend = mock.Mock()
        backend.execute.return_value = (True, "ok", "")
        backend.explain.return_value = (True, "|0 |TABLE SCAN|", "")

        with mock.patch.object(
            perf_comparator,
            "_query_recent_audit_row",
            return_value={
                "ob_elapsed_us": 1000.0,
                "ob_net_time_us": 50.0,
                "ob_plan_type_raw": "1",
                "ob_is_hit_plan": "1",
                "ob_is_executor_rpc": "0",
                "ob_logical_reads": 20.0,
            },
        ), mock.patch.object(
            perf_comparator,
            "collect_plan_monitor_rows",
        ) as monitor_mock:
            replay_row = perf_comparator.replay_statement(config, workload_row, backend=backend)

        monitor_mock.assert_not_called()
        self.assertEqual(replay_row.get("plan_monitor_rows"), [])
        self.assertNotIn("verification_status", replay_row)


class PerfComparatorCliTests(unittest.TestCase):
    def test_check_config_mode_succeeds_with_valid_runtime_paths(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.ini"
            config_path.write_text(
                textwrap.dedent(
                    """
                    [ORACLE_SOURCE]
                    user = scott
                    password = tiger
                    dsn = 127.0.0.1:1521/ORCL

                    [OCEANBASE_TARGET]
                    executable = /bin/echo
                    host = 127.0.0.1
                    port = 2881
                    user_string = root@test#obcluster
                    password = secret

                    [SETTINGS]
                    source_schemas = APP
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = perf_comparator.main(
                    ["--mode", "check-config", "--config", str(config_path)]
                )

            self.assertEqual(exit_code, 0)

    def test_report_only_mode_generates_summary_outputs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            workload_path = Path(tmpdir) / "workload_20260422_153000.jsonl"
            replay_path = Path(tmpdir) / "replay_20260422_153000.jsonl"
            report_dir = Path(tmpdir) / "reports"
            workload_rows = [
                {
                    "sql_id": "sql-1",
                    "sql_text": "SELECT * FROM orders",
                    "oracle_avg_elapsed_us": 1000.0,
                    "oracle_avg_logical_reads": 20.0,
                    "oracle_plan_hash": "ora-1",
                }
            ]
            replay_rows = [
                {
                    "sql_id": "sql-1",
                    "sql_text": "SELECT * FROM orders",
                    "ob_status": "ok",
                    "ob_elapsed_us": 3000.0,
                    "ob_logical_reads": 60.0,
                    "ob_net_time_us": 2100.0,
                    "ob_plan_hash": "ob-1",
                }
            ]
            perf_comparator.append_jsonl(workload_path, workload_rows)
            perf_comparator.append_jsonl(replay_path, replay_rows)
            config_path = Path(tmpdir) / "config.ini"
            config_path.write_text(
                textwrap.dedent(
                    """
                    [ORACLE_SOURCE]
                    user = scott
                    password = tiger
                    dsn = 127.0.0.1:1521/ORCL

                    [OCEANBASE_TARGET]
                    executable = /bin/echo
                    host = 127.0.0.1
                    port = 2881
                    user_string = root@test#obcluster
                    password = secret

                    [SETTINGS]
                    source_schemas = APP
                    workloads_dir = {workloads_dir}
                    report_dir = {report_dir}
                    """
                ).strip().format(workloads_dir=tmpdir, report_dir=report_dir)
                + "\n",
                encoding="utf-8",
            )

            exit_code = perf_comparator.main(
                ["--mode", "report-only", "--config", str(config_path), "--replay", str(replay_path)]
            )

            self.assertEqual(exit_code, 0)
            summary_files = list(report_dir.glob("perf_report_*_summary.txt"))
            html_files = list(report_dir.glob("perf_report_*.html"))
            hints_files = list(report_dir.glob("perf_hints_*.sql"))
            self.assertEqual(len(summary_files), 1)
            self.assertEqual(len(html_files), 1)
            self.assertEqual(len(hints_files), 1)
            self.assertIn("sql-1", summary_files[0].read_text(encoding="utf-8"))

    def test_report_only_mode_matches_fixture_summary(self):
        fixture_path = Path(__file__).parent / "tests" / "fixtures" / "expected_report_summary.txt"
        with tempfile.TemporaryDirectory() as tmpdir:
            workload_path = Path(tmpdir) / "workload_20260422_153000.jsonl"
            replay_path = Path(tmpdir) / "replay_20260422_153000.jsonl"
            report_dir = Path(tmpdir) / "reports"
            perf_comparator.append_jsonl(
                workload_path,
                {
                    "sql_id": "sql-1",
                    "sql_text": "SELECT * FROM orders",
                    "oracle_avg_elapsed_us": 1000.0,
                    "baseline_avg_elapsed_us": 1000.0,
                    "oracle_avg_logical_reads": 20.0,
                    "baseline_avg_logical_reads": 20.0,
                    "oracle_plan_hash": "ora-1",
                },
            )
            perf_comparator.append_jsonl(
                replay_path,
                {
                    "sql_id": "sql-1",
                    "sql_text": "SELECT * FROM orders",
                    "ob_status": "ok",
                    "ob_elapsed_us": 3000.0,
                    "ob_logical_reads": 60.0,
                    "ob_net_time_us": 2100.0,
                    "ob_plan_hash": "ob-1",
                },
            )
            config_path = Path(tmpdir) / "config.ini"
            config_path.write_text(
                textwrap.dedent(
                    """
                    [ORACLE_SOURCE]
                    user = scott
                    password = tiger
                    dsn = 127.0.0.1:1521/ORCL

                    [OCEANBASE_TARGET]
                    executable = /bin/echo
                    host = 127.0.0.1
                    port = 2881
                    user_string = root@test#obcluster
                    password = secret

                    [SETTINGS]
                    source_schemas = APP
                    workloads_dir = {workloads_dir}
                    report_dir = {report_dir}
                    """
                ).strip().format(workloads_dir=tmpdir, report_dir=report_dir)
                + "\n",
                encoding="utf-8",
            )

            exit_code = perf_comparator.main(
                ["--mode", "report-only", "--config", str(config_path), "--replay", str(replay_path)]
            )

            self.assertEqual(exit_code, 0)
            summary_path = next(report_dir.glob("perf_report_*_summary.txt"))
            actual = summary_path.read_text(encoding="utf-8").strip()
            actual = actual.replace(summary_path.name.split("_summary.txt")[0].split("perf_report_")[1], "<RUN_ID>")
            actual = actual.replace(str(replay_path), "<REPLAY_PATH>")
            expected = fixture_path.read_text(encoding="utf-8").strip()
            self.assertEqual(actual, expected)

    def test_report_only_mode_surfaces_verification_and_plan_monitor(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            workload_path = Path(tmpdir) / "workload_20260422_153000.jsonl"
            replay_path = Path(tmpdir) / "replay_20260422_153000.jsonl"
            report_dir = Path(tmpdir) / "reports"
            perf_comparator.append_jsonl(
                workload_path,
                {
                    "sql_id": "sql-1",
                    "sql_text": "SELECT * FROM orders",
                    "baseline_avg_elapsed_us": 1000.0,
                    "baseline_avg_logical_reads": 20.0,
                    "oracle_plan_hash": "ora-1",
                },
            )
            perf_comparator.append_jsonl(
                replay_path,
                {
                    "sql_id": "sql-1",
                    "sql_text": "SELECT * FROM orders",
                    "ob_status": "ok",
                    "ob_elapsed_us": 3000.0,
                    "ob_logical_reads": 60.0,
                    "ob_net_time_us": 2100.0,
                    "ob_plan_hash": "ob-1",
                    "verification_status": "mismatch",
                    "plan_monitor_rows": [
                        {
                            "plan_line_id": 1,
                            "operator": "HASH JOIN",
                            "output_rows": 1000.0,
                            "workarea_mem": 1024.0,
                            "workarea_max_mem": 2048.0,
                            "workarea_tempseg": 64.0,
                        }
                    ],
                },
            )
            config_path = Path(tmpdir) / "config.ini"
            config_path.write_text(
                textwrap.dedent(
                    """
                    [ORACLE_SOURCE]
                    user = scott
                    password = tiger
                    dsn = 127.0.0.1:1521/ORCL

                    [OCEANBASE_TARGET]
                    executable = /bin/echo
                    host = 127.0.0.1
                    port = 2881
                    user_string = root@test#obcluster
                    password = secret

                    [SETTINGS]
                    source_schemas = APP
                    workloads_dir = {workloads_dir}
                    report_dir = {report_dir}
                    """
                ).strip().format(workloads_dir=tmpdir, report_dir=report_dir)
                + "\n",
                encoding="utf-8",
            )
            exit_code = perf_comparator.main(
                ["--mode", "report-only", "--config", str(config_path), "--replay", str(replay_path)]
            )
            self.assertEqual(exit_code, 0)
            html_path = next(report_dir.glob("perf_report_*.html"))
            html_text = html_path.read_text(encoding="utf-8")
            self.assertIn("verification=mismatch", html_text)
            self.assertIn("monitor=HASH JOIN", html_text)

    def test_stream_mode_appends_only_new_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.ini"
            workloads_dir = Path(tmpdir) / "workloads"
            report_dir = Path(tmpdir) / "reports"
            config_path.write_text(
                textwrap.dedent(
                    """
                    [ORACLE_SOURCE]
                    user = scott
                    password = tiger
                    dsn = 127.0.0.1:1521/ORCL

                    [OCEANBASE_TARGET]
                    executable = /bin/echo
                    host = 127.0.0.1
                    port = 2881
                    user_string = root@test#obcluster
                    password = secret

                    [SETTINGS]
                    source_schemas = APP
                    workloads_dir = {workloads_dir}
                    report_dir = {report_dir}
                    duration = 1
                    interval = 1
                    """
                ).strip().format(workloads_dir=workloads_dir, report_dir=report_dir)
                + "\n",
                encoding="utf-8",
            )

            first = [
                {
                    "sql_id": "sql-1",
                    "sql_text": "SELECT 1 FROM dual",
                    "captured_at": "2026-04-22T10:00:00",
                    "oracle_avg_elapsed_us": 1000.0,
                    "oracle_avg_logical_reads": 10.0,
                    "oracle_plan_hash": "a",
                }
            ]
            second = [
                {
                    "sql_id": "sql-2",
                    "sql_text": "SELECT 2 FROM dual",
                    "captured_at": "2026-04-22T10:00:01",
                    "oracle_avg_elapsed_us": 900.0,
                    "oracle_avg_logical_reads": 8.0,
                    "oracle_plan_hash": "b",
                }
            ]

            mock_connection = mock.Mock()
            with mock.patch.object(
                perf_comparator,
                "probe_oracle_capabilities",
                return_value={"awr": False, "vsql": True, "oracle_driver_available": False},
            ), mock.patch.object(
                perf_comparator,
                "probe_replay_capabilities",
                return_value={"obclient": True, "connectivity_ok": True, "explain": True, "sql_audit": False},
            ), mock.patch.object(
                perf_comparator,
                "_open_oracle_connection",
                return_value=mock_connection,
            ), mock.patch.object(
                perf_comparator,
                "_capture_from_vsql",
                side_effect=[first, second],
            ), mock.patch.object(
                perf_comparator.time,
                "sleep",
                return_value=None,
            ), mock.patch.object(
                perf_comparator.time,
                "time",
                side_effect=[0.0, 0.0, 2.0],
            ):
                workload_path = perf_comparator.stream_capture_workload(
                    perf_comparator.load_config(str(config_path)),
                    argparse.Namespace(duration=1, sql_file=None),
                    "20260422_155500",
                )

            rows = perf_comparator.read_jsonl(workload_path)
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["sql_id"], "sql-1")
            self.assertEqual(rows[1]["sql_id"], "sql-2")

    def test_batch_mode_from_sql_file_generates_full_artifacts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.ini"
            sql_file = Path(tmpdir) / "input.sql"
            workloads_dir = Path(tmpdir) / "workloads"
            report_dir = Path(tmpdir) / "reports"
            sql_file.write_text(
                "SELECT * FROM orders WHERE status = 'ACTIVE';\nSELECT * FROM customers;\n",
                encoding="utf-8",
            )
            config_path.write_text(
                textwrap.dedent(
                    """
                    [ORACLE_SOURCE]
                    user = scott
                    password = tiger
                    dsn = 127.0.0.1:1521/ORCL

                    [OCEANBASE_TARGET]
                    executable = /bin/echo
                    host = 127.0.0.1
                    port = 2881
                    user_string = root@test#obcluster
                    password = secret

                    [SETTINGS]
                    source_schemas = APP
                    workloads_dir = {workloads_dir}
                    report_dir = {report_dir}
                    """
                ).strip().format(workloads_dir=workloads_dir, report_dir=report_dir)
                + "\n",
                encoding="utf-8",
            )

            with mock.patch.object(
                perf_comparator,
                "probe_oracle_capabilities",
                return_value={"oracle_driver_available": False, "sql_file": True},
            ):
                exit_code = perf_comparator.main(
                    [
                        "--mode",
                        "batch",
                        "--config",
                        str(config_path),
                        "--sql-file",
                        str(sql_file),
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertEqual(len(list(workloads_dir.glob("workload_*.jsonl"))), 1)
            self.assertEqual(len(list(workloads_dir.glob("replay_*.jsonl"))), 1)
            self.assertEqual(len(list(report_dir.glob("perf_report_*_summary.txt"))), 1)

    def test_replay_only_mode_generates_replay_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.ini"
            workloads_dir = Path(tmpdir) / "workloads"
            workload_path = workloads_dir / "workload_20260422_153000.jsonl"
            workloads_dir.mkdir(parents=True, exist_ok=True)
            perf_comparator.append_jsonl(
                workload_path,
                {
                    "sql_id": "sql-1",
                    "sql_text": "SELECT * FROM orders",
                    "bind_vars": {},
                    "oracle_avg_elapsed_us": 1000.0,
                    "oracle_avg_logical_reads": 10.0,
                    "oracle_plan_hash": "ora-1",
                },
            )
            config_path.write_text(
                textwrap.dedent(
                    """
                    [ORACLE_SOURCE]
                    user = scott
                    password = tiger
                    dsn = 127.0.0.1:1521/ORCL

                    [OCEANBASE_TARGET]
                    executable = /bin/echo
                    host = 127.0.0.1
                    port = 2881
                    user_string = root@test#obcluster
                    password = secret

                    [SETTINGS]
                    source_schemas = APP
                    workloads_dir = {workloads_dir}
                    report_dir = {report_dir}
                    """
                ).strip().format(workloads_dir=workloads_dir, report_dir=Path(tmpdir) / "reports")
                + "\n",
                encoding="utf-8",
            )

            with mock.patch.object(
                perf_comparator,
                "probe_oracle_capabilities",
                return_value={"oracle_driver_available": False, "sql_file": True},
            ):
                exit_code = perf_comparator.main(
                    [
                        "--mode",
                        "replay-only",
                        "--config",
                        str(config_path),
                        "--workload",
                        str(workload_path),
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertEqual(len(list(workloads_dir.glob("replay_*.jsonl"))), 1)

    def test_batch_mode_supports_oceanbase_source_capture(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.ini"
            workloads_dir = Path(tmpdir) / "workloads"
            report_dir = Path(tmpdir) / "reports"
            config_path.write_text(
                textwrap.dedent(
                    """
                    [ORACLE_SOURCE]
                    user = scott
                    password = tiger
                    dsn = 127.0.0.1:1521/ORCL

                    [OCEANBASE_SOURCE]
                    executable = /bin/echo
                    host = 127.0.0.2
                    port = 2883
                    user_string = app@test#obcluster
                    password = source_secret

                    [OCEANBASE_TARGET]
                    executable = /bin/echo
                    host = 127.0.0.1
                    port = 2881
                    user_string = root@test#obcluster
                    password = target_secret

                    [SETTINGS]
                    source_db_mode = oceanbase
                    source_schemas = APP
                    workloads_dir = {workloads_dir}
                    report_dir = {report_dir}
                    duration = 1
                    interval = 1
                    """
                ).strip().format(workloads_dir=workloads_dir, report_dir=report_dir)
                + "\n",
                encoding="utf-8",
            )

            audit_stdout = "101\ttrace-1\tsql-1\t1200\t30\t20\t1150\t800\t20\t3\t1\t1\t90\t4\tSELECT * FROM orders"
            with mock.patch.object(
                perf_comparator,
                "probe_oracle_capabilities",
                return_value={"oracle_driver_available": False, "sql_file": True},
            ), mock.patch.object(
                perf_comparator,
                "probe_replay_capabilities",
                return_value={"obclient": True, "connectivity_ok": True, "explain": True, "sql_audit": False},
            ), mock.patch.object(
                perf_comparator,
                "obclient_run_sql",
                side_effect=[
                    (True, audit_stdout, ""),
                    (True, "", ""),
                    (True, "plan rows", ""),
                    (True, "", ""),
                ],
            ), mock.patch.object(
                perf_comparator.time,
                "sleep",
                return_value=None,
            ), mock.patch.object(
                perf_comparator.time,
                "time",
                side_effect=[0.0, 2.0],
            ):
                exit_code = perf_comparator.main(
                    ["--mode", "batch", "--config", str(config_path)]
                )

            self.assertEqual(exit_code, 0)
            self.assertEqual(len(list(workloads_dir.glob("workload_*.jsonl"))), 1)


if __name__ == "__main__":
    unittest.main()
