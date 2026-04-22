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
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            with self.assertRaises(perf_comparator.ConfigError) as ctx:
                perf_comparator.load_config(str(config_path))

            self.assertIn("OCEANBASE_TARGET", str(ctx.exception))


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


if __name__ == "__main__":
    unittest.main()
