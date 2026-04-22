import argparse
import io
import json
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

    def test_load_config_allows_missing_oracle_in_oceanbase_source_mode(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.ini"
            config_path.write_text(
                textwrap.dedent(
                    """
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
            self.assertEqual(cfg.oracle_source, {})

    def test_load_config_allows_source_only_ob_mode_without_target(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.ini"
            config_path.write_text(
                textwrap.dedent(
                    """
                    [OCEANBASE_SOURCE]
                    executable = /bin/echo
                    host = 127.0.0.2
                    port = 2883
                    user_string = app@test#obcluster
                    password = source_secret

                    [SETTINGS]
                    source_db_mode = oceanbase
                    source_schemas = APP
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            cfg = perf_comparator.load_config(
                str(config_path), execution_mode=perf_comparator.MODE_SOURCE_REPORT
            )

            self.assertEqual(cfg.settings["source_db_mode"], "oceanbase")
            self.assertEqual(cfg.oceanbase_target, {})

    def test_load_config_parses_optional_ob_source_sys_section(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.ini"
            config_path.write_text(
                textwrap.dedent(
                    """
                    [OCEANBASE_SOURCE]
                    executable = /bin/echo
                    host = 127.0.0.2
                    port = 2883
                    user_string = app@test#obcluster
                    password = source_secret

                    [OCEANBASE_SOURCE_SYS]
                    executable = /bin/echo
                    host = 127.0.0.2
                    port = 2883
                    user_string = SYS@ob4ora#observer147
                    password = sys_secret

                    [SETTINGS]
                    source_db_mode = oceanbase
                    source_schemas = APP
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            cfg = perf_comparator.load_config(
                str(config_path), execution_mode=perf_comparator.MODE_SOURCE_REPORT
            )

            self.assertEqual(
                cfg.oceanbase_source_sys["user_string"], "SYS@ob4ora#observer147"
            )

    def test_load_config_parses_optional_external_diagnostics_settings(self):
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
                    ocp_ash_url_template = https://ocp.local/api/ash?sql_id={sql_id}
                    ocp_qpm_url_template = https://ocp.local/api/qpm?sql_id={sql_id}
                    ocp_auth_token_env = PERF_OCP_TOKEN
                    ocp_timeout = 15
                    obdiag_executable = /usr/local/bin/obdiag
                    obdiag_timeout = 45
                    obdiag_extra_args = collect log
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            cfg = perf_comparator.load_config(str(config_path))

            self.assertEqual(
                cfg.settings["ocp_ash_url_template"],
                "https://ocp.local/api/ash?sql_id={sql_id}",
            )
            self.assertEqual(cfg.settings["ocp_qpm_url_template"], "https://ocp.local/api/qpm?sql_id={sql_id}")
            self.assertEqual(cfg.settings["ocp_auth_token_env"], "PERF_OCP_TOKEN")
            self.assertEqual(cfg.settings["ocp_timeout"], 15)
            self.assertEqual(cfg.settings["obdiag_executable"], "/usr/local/bin/obdiag")
            self.assertEqual(cfg.settings["obdiag_timeout"], 45)

    def test_load_config_parses_native_ocp_settings(self):
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
                    ocp_base_url = https://ocp.tidba.com:3600
                    ocp_authorization_env = PERF_OCP_AUTH
                    ocp_cluster_id = 8
                    ocp_tenant_id = 19
                    ocp_verify_tls = false
                    ocp_window_minutes = 30
                    ocp_query_limit = 10
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            cfg = perf_comparator.load_config(str(config_path))

            self.assertEqual(cfg.settings["ocp_base_url"], "https://ocp.tidba.com:3600")
            self.assertEqual(cfg.settings["ocp_authorization_env"], "PERF_OCP_AUTH")
            self.assertEqual(cfg.settings["ocp_cluster_id"], "8")
            self.assertEqual(cfg.settings["ocp_tenant_id"], "19")
            self.assertEqual(cfg.settings["ocp_verify_tls"], False)
            self.assertEqual(cfg.settings["ocp_window_minutes"], 30)
            self.assertEqual(cfg.settings["ocp_query_limit"], 10)

    def test_load_config_parses_native_ocp_auth_and_name_settings(self):
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
                    ocp_base_url = https://ocp.tidba.com:3600
                    ocp_username = admin
                    ocp_password = PAssw0rd01##
                    ocp_cluster_name = observer147
                    ocp_tenant_name = ob4ora
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            cfg = perf_comparator.load_config(str(config_path))

            self.assertEqual(cfg.settings["ocp_username"], "admin")
            self.assertEqual(cfg.settings["ocp_password"], "PAssw0rd01##")
            self.assertEqual(cfg.settings["ocp_cluster_name"], "observer147")
            self.assertEqual(cfg.settings["ocp_tenant_name"], "ob4ora")

    def test_load_config_parses_rolling_source_report_settings(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.ini"
            config_path.write_text(
                textwrap.dedent(
                    """
                    [OCEANBASE_SOURCE]
                    executable = /bin/echo
                    host = 127.0.0.2
                    port = 2883
                    user_string = app@test#obcluster
                    password = source_secret

                    [SETTINGS]
                    source_db_mode = oceanbase
                    source_schemas = APP
                    rolling_report_interval = 120
                    source_actor_fields = tenant_name,db_name,user_name,user_client_ip
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            cfg = perf_comparator.load_config(
                str(config_path), execution_mode=perf_comparator.MODE_SOURCE_REPORT
            )

            self.assertEqual(cfg.settings["rolling_report_interval"], 120)
            self.assertEqual(
                cfg.settings["source_actor_fields"],
                ["tenant_name", "db_name", "user_name", "user_client_ip"],
            )


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

    def test_render_sql_for_replay_normalizes_call_statements(self):
        rendered, skip_reason = perf_comparator.render_sql_for_replay(
            "CALL test_profiler_pkg.run_profile_workload()",
            {},
        )
        self.assertIsNone(skip_reason)
        self.assertEqual(rendered, "BEGIN test_profiler_pkg.run_profile_workload(); END")

    def test_render_sql_for_replay_returns_skip_for_unsupported_bind_types(self):
        rendered, skip_reason = perf_comparator.render_sql_for_replay(
            "SELECT * FROM orders WHERE payload = :1",
            {"1": {"complex": "value"}},
        )
        self.assertIsNone(rendered)
        self.assertIn("unsupported bind types", skip_reason)

    def test_build_source_actor_key_uses_configured_fields(self):
        row = {
            "source_tenant_name": "ob4ora",
            "source_db_name": "observer147",
            "source_user_name": "QA_FINANCE",
            "source_user_client_ip": "172.16.0.51",
        }
        actor_key = perf_comparator.build_source_actor_key(
            row, ["tenant_name", "db_name", "user_name", "user_client_ip"]
        )
        self.assertIn("tenant_name=ob4ora", actor_key)
        self.assertIn("user_name=QA_FINANCE", actor_key)

    def test_maybe_refresh_source_report_regenerates_snapshot_when_due(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            workload_path = Path(tmpdir) / "workload.jsonl"
            workload_path.write_text("{}", encoding="utf-8")
            config = perf_comparator.AppConfig(
                oracle_source={},
                oceanbase_source={},
                oceanbase_target={},
                settings={
                    "source_schemas": ["APP"],
                    "report_dir": tmpdir,
                    "workloads_dir": tmpdir,
                    "rolling_report_interval": 60,
                    "source_actor_fields": ["tenant_name", "db_name", "user_name", "user_client_ip"],
                },
                config_path="config.ini",
            )

            with mock.patch.object(
                perf_comparator,
                "generate_report_from_source_workload",
                return_value={"summary": Path(tmpdir) / "summary.txt"},
            ) as report_mock, mock.patch.object(
                perf_comparator.time,
                "time",
                return_value=180.0,
            ):
                refreshed_at = perf_comparator.maybe_refresh_source_report(
                    config, workload_path, "20260422_rolling", 100.0
                )

        report_mock.assert_called_once()
        self.assertEqual(refreshed_at, 180.0)

    def test_build_source_ob_audit_query_with_caller_fields_has_no_trailing_comma(self):
        query = perf_comparator._build_source_ob_audit_query(0)
        self.assertIn("RET_CODE\n        FROM GV$OB_SQL_AUDIT", query)
        self.assertNotIn("RET_CODE,\n        FROM GV$OB_SQL_AUDIT", query)
        self.assertIn("TENANT_NAME", query)

        legacy_query = perf_comparator._build_source_ob_audit_query(
            0, include_caller_fields=False
        )
        self.assertNotIn("TENANT_NAME", legacy_query)

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

    def test_backfill_source_workload_sql_texts_uses_source_sys_connection(self):
        config = perf_comparator.AppConfig(
            oracle_source={},
            oceanbase_source={
                "executable": "/bin/echo",
                "host": "127.0.0.2",
                "port": "2883",
                "user_string": "app@test#obcluster",
                "password": "source_secret",
            },
            oceanbase_target={},
            settings={"source_db_mode": "oceanbase", "source_schemas": ["APP"]},
            config_path="config.ini",
            oceanbase_source_sys={
                "executable": "/bin/echo",
                "host": "127.0.0.2",
                "port": "2883",
                "user_string": "SYS@ob4ora#observer147",
                "password": "sys_secret",
            },
        )
        rows = [
            {
                "sql_id": "sql-1",
                "sql_text": "NULL",
                "sql_text_normalized": "NULL",
                "schema": "APP",
            }
        ]

        with mock.patch.object(
            perf_comparator,
            "obclient_run_sql",
            return_value=(
                True,
                "sql-1\tSELECT /* recovered */ * FROM orders",
                "",
            ),
        ) as obclient_mock:
            enriched_rows, stats = perf_comparator.backfill_source_workload_sql_texts(
                config, rows
            )

        obclient_mock.assert_called_once()
        self.assertEqual(
            obclient_mock.call_args[0][0]["user_string"], "SYS@ob4ora#observer147"
        )
        self.assertEqual(
            enriched_rows[0]["sql_text"], "SELECT /* recovered */ * FROM orders"
        )
        self.assertEqual(enriched_rows[0]["source_sql_text_status"], "backfilled")
        self.assertEqual(stats["backfilled"], 1)
        self.assertEqual(stats["lookup_user"], "SYS@ob4ora#observer147")

    def test_backfill_source_workload_sql_texts_falls_back_to_native_ocp(self):
        config = perf_comparator.AppConfig(
            oracle_source={},
            oceanbase_source={
                "executable": "/bin/echo",
                "host": "127.0.0.2",
                "port": "2883",
                "user_string": "app@test#obcluster",
                "password": "source_secret",
            },
            oceanbase_target={},
            settings={
                "source_db_mode": "oceanbase",
                "source_schemas": ["APP"],
                "ocp_base_url": "https://ocp.tidba.com:3600",
                "ocp_username": "admin",
                "ocp_password": "PAssw0rd01##",
                "ocp_cluster_name": "observer147",
                "ocp_tenant_name": "ob4ora",
                "report_dir": ".",
            },
            config_path="config.ini",
            oceanbase_source_sys={},
        )
        rows = [
            {
                "sql_id": "A1B2C3",
                "sql_text": "NULL",
                "sql_text_normalized": "NULL",
                "schema": "APP",
            }
        ]

        with mock.patch.object(
            perf_comparator,
            "lookup_source_sql_texts",
            return_value={},
        ), mock.patch.object(
            perf_comparator,
            "lookup_sql_texts_via_ocp_native",
            return_value={"A1B2C3": "SELECT * FROM orders WHERE status = 'ACTIVE'"},
        ):
            enriched_rows, stats = perf_comparator.backfill_source_workload_sql_texts(
                config, rows
            )

        self.assertEqual(
            enriched_rows[0]["sql_text"], "SELECT * FROM orders WHERE status = 'ACTIVE'"
        )
        self.assertEqual(enriched_rows[0]["source_sql_text_status"], "backfilled")
        self.assertEqual(enriched_rows[0]["source_sql_text_source"], "ocp_native")
        self.assertEqual(stats["backfilled_via_ocp_native"], 1)

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

    def test_build_recommendations_emits_executable_dist_join_and_plan_miss_templates(self):
        row = {
            "sql_id": "sql-join-1",
            "sql_text": "SELECT * FROM orders o JOIN order_items i ON i.order_id = o.id",
            "ob_status": "ok",
            "speedup_ratio": 0.4,
            "net_ratio": 0.8,
            "plan_changed": False,
            "ob_is_executor_rpc": "1",
            "ob_queue_time_us": 100.0,
            "ob_execute_time_us": 1000.0,
            "ob_retry_cnt": 0,
            "ob_is_hit_plan": "0",
            "ob_get_plan_time_us": 400.0,
            "ob_elapsed_us": 1200.0,
            "ob_memstore_read_rows": 100.0,
            "ob_ssstore_read_rows": 50.0,
            "ob_bloom_filter_filtered": 0.0,
            "schema": "APP",
        }

        recommendations = perf_comparator.build_recommendations(row, slowdown_threshold=0.8)
        recommendation_map = {item["rule_id"]: item for item in recommendations}

        self.assertIn("CREATE TABLEGROUP", recommendation_map["DIST-JOIN"]["hint_sql"])
        self.assertIn("ALTER TABLE orders SET TABLEGROUP", recommendation_map["DIST-JOIN"]["hint_sql"])
        self.assertIn("ALTER TABLE order_items SET TABLEGROUP", recommendation_map["DIST-JOIN"]["hint_sql"])
        self.assertIn("DBMS_STATS.GATHER_TABLE_STATS", recommendation_map["PLAN-MISS"]["hint_sql"])
        self.assertIn("CALL DBMS_XPLAN.ENABLE_OPT_TRACE()", recommendation_map["PLAN-MISS"]["hint_sql"])

    def test_build_recommendations_links_plsql_rpc_to_profiler_hotspot(self):
        row = {
            "sql_id": "pkg-1",
            "sql_text": "BEGIN insurance_workload_pkg_small.run_profile_workload; END",
            "ob_status": "ok",
            "speedup_ratio": 0.5,
            "net_ratio": 0.9,
            "plan_changed": False,
            "ob_is_executor_rpc": "1",
            "ob_queue_time_us": 100.0,
            "ob_execute_time_us": 1000.0,
            "ob_retry_cnt": 0,
            "ob_is_hit_plan": "1",
            "ob_get_plan_time_us": 10.0,
            "ob_elapsed_us": 1200.0,
            "ob_memstore_read_rows": 100.0,
            "ob_ssstore_read_rows": 50.0,
            "ob_bloom_filter_filtered": 0.0,
            "plsql_profile_status": "ok",
            "plsql_profile_top_lines": [
                {
                    "owner": "OMS_USER",
                    "unit_name": "INSURANCE_WORKLOAD_PKG_SMALL",
                    "unit_type": "PACKAGE BODY",
                    "line": 87,
                    "total_time_us": 820000.0,
                    "source_text": "FOR i IN 1..v_ids.COUNT LOOP",
                    "context_lines": [
                        {"line": 86, "text": "FOR i IN 1..v_ids.COUNT LOOP"},
                        {"line": 87, "text": "UPDATE orders SET status = 'DONE' WHERE id = v_ids(i);"},
                    ],
                }
            ],
        }

        recommendations = perf_comparator.build_recommendations(row, slowdown_threshold=0.8)
        recommendation_map = {item["rule_id"]: item for item in recommendations}

        self.assertIn("line 87", recommendation_map["PLSQL-RPC"]["message"])
        self.assertIn("FORALL", recommendation_map["PLSQL-RPC"]["hint_sql"])
        self.assertIn("MERGE INTO", recommendation_map["PLSQL-RPC"]["hint_sql"])
        self.assertIn("INSURANCE_WORKLOAD_PKG_SMALL", recommendation_map["PLSQL-RPC"]["hint_sql"])

    def test_analyze_plsql_profile_evidence_detects_complex_package_patterns(self):
        hot_lines = [
            {
                "owner": "OMS_USER",
                "unit_name": "ORDER_SYNC_PKG",
                "unit_type": "PACKAGE BODY",
                "line": 87,
                "total_occur": 20000,
                "total_time_us": 1800000.0,
                "source_text": "FOR rec IN c_orders LOOP",
                "context_lines": [
                    {"line": 86, "text": "FOR rec IN c_orders LOOP"},
                    {"line": 87, "text": "  v_sql := 'UPDATE orders SET status = :1 WHERE id = :2';"},
                    {"line": 88, "text": "  EXECUTE IMMEDIATE v_sql USING 'DONE', rec.order_id;"},
                    {"line": 89, "text": "  UPDATE order_items SET synced_flag = 'Y' WHERE order_id = rec.order_id;"},
                    {"line": 90, "text": "  COMMIT;"},
                    {"line": 91, "text": "END LOOP;"},
                ],
            },
            {
                "owner": "OMS_USER",
                "unit_name": "ORDER_SYNC_PKG",
                "unit_type": "PACKAGE BODY",
                "line": 88,
                "total_occur": 20000,
                "total_time_us": 2400000.0,
                "source_text": "EXECUTE IMMEDIATE v_sql USING 'DONE', rec.order_id;",
                "context_lines": [
                    {"line": 87, "text": "FOR rec IN c_orders LOOP"},
                    {"line": 88, "text": "  EXECUTE IMMEDIATE v_sql USING 'DONE', rec.order_id;"},
                    {"line": 89, "text": "  UPDATE order_items SET synced_flag = 'Y' WHERE order_id = rec.order_id;"},
                    {"line": 90, "text": "  COMMIT;"},
                ],
            },
            {
                "owner": "OMS_USER",
                "unit_name": "ORDER_SYNC_PKG",
                "unit_type": "PACKAGE BODY",
                "line": 90,
                "total_occur": 20000,
                "total_time_us": 900000.0,
                "source_text": "COMMIT;",
                "context_lines": [
                    {"line": 88, "text": "  EXECUTE IMMEDIATE v_sql USING 'DONE', rec.order_id;"},
                    {"line": 89, "text": "  UPDATE order_items SET synced_flag = 'Y' WHERE order_id = rec.order_id;"},
                    {"line": 90, "text": "  COMMIT;"},
                    {"line": 91, "text": "END LOOP;"},
                ],
            },
            {
                "owner": "OMS_USER",
                "unit_name": "ORDER_SYNC_PKG",
                "unit_type": "PACKAGE BODY",
                "line": 140,
                "total_occur": 500000,
                "total_time_us": 700000.0,
                "source_text": "v_acc := v_acc + SQRT(i);",
                "context_lines": [
                    {"line": 139, "text": "FOR i IN 1..100000 LOOP"},
                    {"line": 140, "text": "  v_acc := v_acc + SQRT(i);"},
                    {"line": 141, "text": "END LOOP;"},
                ],
            },
        ]
        unit_summary = [
            {
                "owner": "OMS_USER",
                "unit_name": "ORDER_SYNC_PKG",
                "unit_type": "PACKAGE BODY",
                "total_time_us": 6500000.0,
                "total_occur": 560000,
                "profile_time_ratio": 1.0,
            }
        ]

        analysis = perf_comparator.analyze_plsql_profile_evidence(hot_lines, unit_summary)

        self.assertEqual(analysis["unit_summary"][0]["unit_name"], "ORDER_SYNC_PKG")
        self.assertEqual(analysis["hot_blocks"][0]["start_line"], 87)
        self.assertEqual(analysis["hot_blocks"][0]["end_line"], 90)
        diagnosis_ids = [item["diagnosis_id"] for item in analysis["diagnoses"]]
        self.assertIn("dynamic_sql_in_loop", diagnosis_ids)
        self.assertIn("frequent_commit_in_loop", diagnosis_ids)
        self.assertIn("row_by_row_sql_in_loop", diagnosis_ids)
        self.assertIn("tight_cpu_loop", diagnosis_ids)
        self.assertIn("ORDER_SYNC_PKG:87-90:dynamic_sql_in_loop", analysis["diagnosis_summary"])

    def test_analyze_plsql_profile_evidence_ignores_insignificant_noise_blocks(self):
        hot_lines = [
            {
                "owner": "OMS_USER",
                "unit_name": "ORDER_SYNC_PKG",
                "unit_type": "PACKAGE BODY",
                "line": 50,
                "total_occur": 1000,
                "total_time_us": 5000000.0,
                "source_text": "EXECUTE IMMEDIATE v_stmt USING rec.id;",
                "context_lines": [
                    {"line": 49, "text": "FOR rec IN c_orders LOOP"},
                    {"line": 50, "text": "  EXECUTE IMMEDIATE v_stmt USING rec.id;"},
                    {"line": 51, "text": "END LOOP;"},
                ],
            },
            {
                "owner": "OMS_USER",
                "unit_name": "ORDER_SYNC_PKG",
                "unit_type": "PACKAGE BODY",
                "line": 8,
                "total_occur": 1,
                "total_time_us": 10.0,
                "source_text": "IF v_seed_count < 400 THEN",
                "context_lines": [
                    {"line": 8, "text": "IF v_seed_count < 400 THEN"},
                    {"line": 9, "text": "  DELETE FROM t_case;"},
                    {"line": 10, "text": "  FOR i IN 1..400 LOOP"},
                ],
            },
        ]
        analysis = perf_comparator.analyze_plsql_profile_evidence(hot_lines, [])
        diagnosis_summaries = [item["line_range"] for item in analysis["diagnoses"]]
        self.assertIn("50", diagnosis_summaries)
        self.assertNotIn("8", diagnosis_summaries)

    def test_build_recommendations_emits_diagnosis_aware_plsql_rules(self):
        row = {
            "sql_id": "pkg-diag-1",
            "sql_text": "BEGIN order_sync_pkg.run_batch; END",
            "ob_status": "ok",
            "speedup_ratio": 0.4,
            "net_ratio": 0.7,
            "plan_changed": False,
            "ob_is_executor_rpc": "1",
            "ob_queue_time_us": 200.0,
            "ob_execute_time_us": 1000.0,
            "ob_retry_cnt": 0,
            "ob_is_hit_plan": "1",
            "ob_get_plan_time_us": 20.0,
            "ob_elapsed_us": 1200.0,
            "ob_memstore_read_rows": 100.0,
            "ob_ssstore_read_rows": 20.0,
            "ob_bloom_filter_filtered": 0.0,
            "plsql_profile_status": "ok",
            "plsql_profile_diagnosis_summary": "ORDER_SYNC_PKG:87-90:dynamic_sql_in_loop",
            "plsql_profile_diagnoses": [
                {
                    "diagnosis_id": "dynamic_sql_in_loop",
                    "unit_name": "ORDER_SYNC_PKG",
                    "line_range": "87-90",
                    "message": "Dynamic SQL is executed inside a hot loop.",
                },
                {
                    "diagnosis_id": "frequent_commit_in_loop",
                    "unit_name": "ORDER_SYNC_PKG",
                    "line_range": "87-90",
                    "message": "Commit appears inside a high-frequency loop.",
                },
            ],
            "plsql_profile_top_lines": [
                {
                    "owner": "OMS_USER",
                    "unit_name": "ORDER_SYNC_PKG",
                    "unit_type": "PACKAGE BODY",
                    "line": 88,
                    "total_time_us": 2400000.0,
                    "source_text": "EXECUTE IMMEDIATE v_sql USING 'DONE', rec.order_id;",
                    "context_lines": [
                        {"line": 87, "text": "FOR rec IN c_orders LOOP"},
                        {"line": 88, "text": "  EXECUTE IMMEDIATE v_sql USING 'DONE', rec.order_id;"},
                        {"line": 89, "text": "  UPDATE order_items SET synced_flag = 'Y' WHERE order_id = rec.order_id;"},
                        {"line": 90, "text": "  COMMIT;"},
                    ],
                }
            ],
        }

        recommendations = perf_comparator.build_recommendations(row, slowdown_threshold=0.8)
        recommendation_map = {item["rule_id"]: item for item in recommendations}

        self.assertIn("PLSQL-DYNAMIC-SQL", recommendation_map)
        self.assertIn("PLSQL-COMMIT-HOT", recommendation_map)
        self.assertIn("dynamic SQL", recommendation_map["PLSQL-DYNAMIC-SQL"]["message"])
        self.assertIn("COMMIT", recommendation_map["PLSQL-COMMIT-HOT"]["hint_sql"].upper())

    def test_build_plsql_profiler_payload_uses_begin_end_wrappers(self):
        payload = perf_comparator._build_plsql_profiler_payload(
            "CALL TEST_PROFILER_PKG.run_profile_workload()",
            "pc_case_1",
        )
        self.assertTrue(payload.startswith("BEGIN\n"))
        self.assertIn("DBMS_PROFILER.START_PROFILER('pc_case_1');", payload)
        self.assertIn("CALL TEST_PROFILER_PKG.run_profile_workload();", payload)
        self.assertIn("DBMS_PROFILER.STOP_PROFILER();", payload)
        self.assertTrue(payload.rstrip().endswith("END;"))


class PerfComparatorOracleCaptureTests(unittest.TestCase):
    def _build_config(self, tmpdir, **settings_updates):
        settings = {
            "source_schemas": ["APP"],
            "hours": 24,
            "min_exec": 5,
            "top_n": 50,
            "workloads_dir": tmpdir,
            "report_dir": str(Path(tmpdir) / "reports"),
        }
        settings.update(settings_updates)
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
            settings=settings,
            config_path="config.ini",
        )

    def test_capture_workload_falls_back_to_unified_audit(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = self._build_config(tmpdir)
            mock_connection = mock.Mock()
            expected_rows = [
                {
                    "sql_id": "ua-1",
                    "sql_text": "SELECT * FROM orders",
                    "schema": "APP",
                    "source": "unified_audit",
                }
            ]

            with mock.patch.object(
                perf_comparator,
                "probe_oracle_capabilities",
                return_value={"awr": False, "vsql": False, "unified_audit": True, "wcr": False},
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
                "_capture_from_unified_audit",
                return_value=expected_rows,
            ) as audit_mock:
                workload_path = perf_comparator.capture_workload(
                    config, argparse.Namespace(sql_file=None, wcr_path=None), "20260422_190000"
                )

            audit_mock.assert_called_once_with(mock_connection, config)
            rows = perf_comparator.read_jsonl(workload_path)
            self.assertEqual(rows[0]["source"], "unified_audit")

    def test_capture_workload_falls_back_to_wcr(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            wcr_path = Path(tmpdir) / "sample.wcr"
            wcr_path.write_text("SELECT * FROM invoices;\n", encoding="utf-8")
            config = self._build_config(tmpdir, wcr_path=str(wcr_path))

            with mock.patch.object(
                perf_comparator,
                "probe_oracle_capabilities",
                return_value={"awr": False, "vsql": False, "unified_audit": False, "wcr": True},
            ), mock.patch.object(
                perf_comparator,
                "probe_replay_capabilities",
                return_value={"obclient": True, "connectivity_ok": True, "explain": True, "sql_audit": False},
            ), mock.patch.object(
                perf_comparator,
                "capture_from_wcr_file",
                return_value=Path(tmpdir) / "workloads" / "workload_20260422_190001.jsonl",
            ) as wcr_mock:
                workload_path = perf_comparator.capture_workload(
                    config, argparse.Namespace(sql_file=None, wcr_path=None), "20260422_190001"
                )

            wcr_mock.assert_called_once()
            self.assertEqual(str(workload_path).endswith(".jsonl"), True)

    def test_capture_workload_prefers_awr_before_wcr(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            wcr_path = Path(tmpdir) / "sample.wcr"
            wcr_path.write_text("SELECT * FROM invoices;\n", encoding="utf-8")
            config = self._build_config(tmpdir, wcr_path=str(wcr_path))
            mock_connection = mock.Mock()
            expected_rows = [
                {
                    "sql_id": "awr-1",
                    "sql_text": "SELECT * FROM orders",
                    "schema": "APP",
                    "source": "awr",
                }
            ]

            with mock.patch.object(
                perf_comparator,
                "probe_oracle_capabilities",
                return_value={"awr": True, "vsql": True, "unified_audit": True, "wcr": True},
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
                "_capture_from_awr",
                return_value=expected_rows,
            ) as awr_mock, mock.patch.object(
                perf_comparator,
                "capture_from_wcr_file",
            ) as wcr_mock:
                workload_path = perf_comparator.capture_workload(
                    config, argparse.Namespace(sql_file=None, wcr_path=None), "20260422_190002"
                )

            awr_mock.assert_called_once_with(mock_connection, config)
            wcr_mock.assert_not_called()
            rows = perf_comparator.read_jsonl(workload_path)
            self.assertEqual(rows[0]["source"], "awr")

    def test_build_recommendations_handles_skip_status(self):
        row = {"ob_status": "skip", "speedup_ratio": None, "net_ratio": None}
        recommendations = perf_comparator.build_recommendations(row, slowdown_threshold=0.8)
        self.assertEqual(recommendations[0]["rule_id"], "REPLAY-SKIP")

    def test_parse_ob_audit_rows_into_workload_events(self):
        stdout = (
            "101\ttrace-1\tsql-1\t1200\t30\t20\t1150\t800\t20\t3\t1\t1\t0\t2\t11\t90\t700\t50\t12\t4\tSELECT * FROM orders\n"
            "102\ttrace-2\tsql-2\t600\t10\t10\t580\t100\t5\t1\t1\t0\t1\t0\t2\t20\t10\t2\t5\t1\tBEGIN pkg.run(); END"
        )
        rows = perf_comparator.parse_ob_audit_rows(stdout, "APP", captured_at="2026-04-22T15:00:00Z")
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["sql_id"], "sql-1")
        self.assertEqual(rows[0]["source"], "ob_sql_audit")
        self.assertEqual(rows[0]["baseline_source_mode"], "oceanbase")
        self.assertEqual(rows[0]["oracle_avg_elapsed_us"], 1200.0)
        self.assertEqual(rows[0]["source_ob_retry_cnt"], 2.0)
        self.assertEqual(rows[0]["source_ob_memstore_read_rows"], 700.0)
        self.assertEqual(rows[0]["source_ob_block_cache_hit"], 90.0)
        self.assertEqual(rows[1]["sql_text"], "BEGIN pkg.run(); END")

    def test_parse_ob_audit_rows_preserves_caller_attribution_fields(self):
        stdout = (
            "201\ttrace-201\tsql-201\t2400\t40\t30\t2300\t1200\t30\t3\t1\t1\t1\t2\t99\t95\t500\t80\t20\t6\tSELECT * FROM orders\tob4ora\tobserver147\tOMS_USER\t172.16.0.201\t10.10.1.5\t0"
        )
        rows = perf_comparator.parse_ob_audit_rows(stdout, "APP", captured_at="2026-04-22T20:00:00Z")
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["source_tenant_name"], "ob4ora")
        self.assertEqual(row["source_db_name"], "observer147")
        self.assertEqual(row["source_user_name"], "OMS_USER")
        self.assertEqual(row["source_user_client_ip"], "172.16.0.201")
        self.assertEqual(row["source_client_ip"], "10.10.1.5")
        self.assertEqual(row["source_ret_code"], 0.0)

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

    def test_build_plan_diff_signals_detects_lookup_risk(self):
        row = {
            "oracle_plan_rows": [
                {"id": 1, "operation": "TABLE ACCESS", "options": "BY INDEX ROWID", "object_name": "ORDERS"},
                {"id": 2, "operation": "INDEX", "options": "RANGE SCAN", "object_name": "IDX_ORDERS_STATUS"},
            ],
            "ob_plan_rows": [
                {"id": 1, "operator": "TABLE LOOKUP", "name": "ORDERS"},
                {"id": 2, "operator": "TABLE SCAN", "name": "ORDERS"},
            ],
            "ob_plan_type_raw": "3",
            "plan_monitor_rows": [],
        }
        signals = perf_comparator.build_plan_diff_signals(row)
        self.assertTrue(any(item["signal_id"] == "LOOKUP-RISK" for item in signals))

    def test_aggregate_source_workload_rows_preserves_hotspot_signals(self):
        rows = [
            {
                "sql_id": "sql-1",
                "sql_text": "SELECT * FROM orders",
                "sql_text_normalized": "SELECT * FROM ORDERS",
                "baseline_avg_elapsed_us": 1000.0,
                "oracle_avg_elapsed_us": 1000.0,
                "oracle_avg_logical_reads": 100.0,
                "oracle_avg_physical_reads": 10.0,
                "source_ob_queue_time_us": 200.0,
                "source_ob_get_plan_time_us": 100.0,
                "source_ob_execute_time_us": 700.0,
                "source_ob_net_time_us": 650.0,
                "source_ob_net_wait_time_us": 20.0,
                "source_ob_plan_type_raw": "3",
                "source_ob_is_hit_plan": "0",
                "source_ob_is_executor_rpc": "1",
                "source_ob_retry_cnt": 1.0,
                "source_ob_memstore_read_rows": 400.0,
                "source_ob_ssstore_read_rows": 100.0,
                "source_ob_bloom_filter_filtered": 0.0,
                "source_ob_trace_id": "trace-1",
                "source_ob_request_id": "100",
            },
            {
                "sql_id": "sql-1",
                "sql_text": "SELECT * FROM orders",
                "sql_text_normalized": "SELECT * FROM ORDERS",
                "baseline_avg_elapsed_us": 2000.0,
                "oracle_avg_elapsed_us": 2000.0,
                "oracle_avg_logical_reads": 120.0,
                "oracle_avg_physical_reads": 12.0,
                "source_ob_queue_time_us": 300.0,
                "source_ob_get_plan_time_us": 300.0,
                "source_ob_execute_time_us": 1400.0,
                "source_ob_net_time_us": 1300.0,
                "source_ob_net_wait_time_us": 40.0,
                "source_ob_plan_type_raw": "3",
                "source_ob_is_hit_plan": "0",
                "source_ob_is_executor_rpc": "1",
                "source_ob_retry_cnt": 2.0,
                "source_ob_memstore_read_rows": 500.0,
                "source_ob_ssstore_read_rows": 100.0,
                "source_ob_bloom_filter_filtered": 0.0,
                "source_ob_trace_id": "trace-2",
                "source_ob_request_id": "101",
            },
        ]
        aggregated = perf_comparator.aggregate_ob_source_workload_rows(rows)
        self.assertEqual(len(aggregated), 1)
        row = aggregated[0]
        self.assertEqual(row["source_sample_count"], 2)
        self.assertEqual(row["ob_plan_type_raw"], "3")
        self.assertAlmostEqual(row["ob_elapsed_us"], 1500.0)
        self.assertAlmostEqual(row["net_ratio"], 1950.0 / 3000.0)
        self.assertEqual(row["source_ob_trace_id"], "trace-2")

    def test_aggregate_source_workload_rows_supports_snapshot_execution_counts(self):
        rows = [
            {
                "sql_id": "sqlstat-1",
                "sql_text": "SELECT /* from sqlstat */ 1 FROM dual",
                "sql_text_normalized": "SELECT /* FROM SQLSTAT */ 1 FROM DUAL",
                "source_execution_count": 5,
                "source_total_elapsed_us": 5000.0,
                "baseline_avg_elapsed_us": 1000.0,
                "oracle_avg_elapsed_us": 1000.0,
                "oracle_avg_logical_reads": 50.0,
                "oracle_avg_physical_reads": 5.0,
                "source_ob_logical_reads": 50.0,
                "source_ob_physical_reads": 5.0,
                "source_ob_queue_time_us": 0.0,
                "source_ob_get_plan_time_us": 50.0,
                "source_ob_execute_time_us": 950.0,
                "source_ob_net_time_us": 100.0,
                "source_ob_net_wait_time_us": 0.0,
                "source_ob_plan_type_raw": "1",
                "source_ob_is_hit_plan": "1",
                "source_ob_is_executor_rpc": "0",
                "source_ob_retry_cnt": 0.0,
                "source_ob_memstore_read_rows": 20.0,
                "source_ob_ssstore_read_rows": 5.0,
                "source_ob_trace_id": None,
                "source_ob_request_id": None,
            }
        ]

        aggregated = perf_comparator.aggregate_ob_source_workload_rows(rows)

        self.assertEqual(len(aggregated), 1)
        row = aggregated[0]
        self.assertEqual(row["source_sample_count"], 5)
        self.assertAlmostEqual(row["source_total_elapsed_us"], 5000.0)
        self.assertAlmostEqual(row["ob_elapsed_us"], 1000.0)

    def test_aggregate_source_workload_rows_preserves_sql_text_source(self):
        rows = [
            {
                "sql_id": "sql-1",
                "sql_text": "SELECT * FROM orders",
                "source_execution_count": 1,
                "baseline_avg_elapsed_us": 1000.0,
                "oracle_avg_elapsed_us": 1000.0,
                "source_sql_text_source": "ocp_native",
            },
            {
                "sql_id": "sql-1",
                "sql_text": "SELECT * FROM orders",
                "source_execution_count": 1,
                "baseline_avg_elapsed_us": 1000.0,
                "oracle_avg_elapsed_us": 1000.0,
                "source_sql_text_source": "captured",
            },
        ]

        aggregated = perf_comparator.aggregate_ob_source_workload_rows(rows)

        self.assertEqual(aggregated[0]["source_sql_text_source"], "captured")

    def test_aggregate_source_workload_rows_tracks_primary_actor_and_plsql_type(self):
        rows = [
            {
                "sql_id": "pkg-actor-1",
                "sql_text": "BEGIN billing_pkg.run_close_day; END",
                "source_execution_count": 3,
                "baseline_avg_elapsed_us": 6000.0,
                "oracle_avg_elapsed_us": 6000.0,
                "source_ob_queue_time_us": 300.0,
                "source_ob_get_plan_time_us": 50.0,
                "source_ob_execute_time_us": 5650.0,
                "source_ob_net_time_us": 2000.0,
                "source_ob_plan_type_raw": "3",
                "source_ob_is_hit_plan": "1",
                "source_ob_is_executor_rpc": "1",
                "source_tenant_name": "ob4ora",
                "source_db_name": "observer147",
                "source_user_name": "QA_FINANCE",
                "source_user_client_ip": "172.16.0.51",
            },
            {
                "sql_id": "pkg-actor-1",
                "sql_text": "BEGIN billing_pkg.run_close_day; END",
                "source_execution_count": 2,
                "baseline_avg_elapsed_us": 5000.0,
                "oracle_avg_elapsed_us": 5000.0,
                "source_ob_queue_time_us": 200.0,
                "source_ob_get_plan_time_us": 40.0,
                "source_ob_execute_time_us": 4760.0,
                "source_ob_net_time_us": 1800.0,
                "source_ob_plan_type_raw": "3",
                "source_ob_is_hit_plan": "1",
                "source_ob_is_executor_rpc": "1",
                "source_tenant_name": "ob4ora",
                "source_db_name": "observer147",
                "source_user_name": "QA_FINANCE",
                "source_user_client_ip": "172.16.0.51",
            },
        ]

        aggregated = perf_comparator.aggregate_ob_source_workload_rows(rows)

        self.assertEqual(len(aggregated), 1)
        row = aggregated[0]
        self.assertEqual(row["source_workload_type"], "plsql")
        self.assertEqual(row["source_primary_actor_count"], 5)
        self.assertIn("QA_FINANCE", row["source_primary_actor"])
        self.assertEqual(row["source_actor_count"], 1)

    def test_build_source_sqlstat_delta_rows_emits_missing_sql_ids(self):
        start_snapshot = {
            "sql-keep": {
                "sql_id": "sql-keep",
                "query_sql": "SELECT 1 FROM dual",
                "plan_type": "1",
                "executions_total": 10.0,
                "elapsed_time_total": 10000.0,
                "buffer_gets_total": 500.0,
                "disk_reads_total": 50.0,
                "memstore_read_rows_total": 100.0,
                "minor_ssstore_read_rows_total": 20.0,
                "major_ssstore_read_rows_total": 30.0,
                "rpc_total": 0.0,
                "retry_total": 0.0,
                "plan_cache_hit_total": 10.0,
            }
        }
        end_snapshot = {
            "sql-keep": {
                "sql_id": "sql-keep",
                "query_sql": "SELECT 1 FROM dual",
                "plan_type": "1",
                "executions_total": 12.0,
                "elapsed_time_total": 12000.0,
                "buffer_gets_total": 520.0,
                "disk_reads_total": 52.0,
                "memstore_read_rows_total": 110.0,
                "minor_ssstore_read_rows_total": 21.0,
                "major_ssstore_read_rows_total": 31.0,
                "rpc_total": 0.0,
                "retry_total": 0.0,
                "plan_cache_hit_total": 12.0,
            },
            "sql-new": {
                "sql_id": "sql-new",
                "query_sql": "SELECT /* sqlstat supplement */ * FROM orders",
                "plan_type": "3",
                "executions_total": 4.0,
                "elapsed_time_total": 4000.0,
                "buffer_gets_total": 600.0,
                "disk_reads_total": 40.0,
                "memstore_read_rows_total": 200.0,
                "minor_ssstore_read_rows_total": 30.0,
                "major_ssstore_read_rows_total": 10.0,
                "rpc_total": 4.0,
                "retry_total": 2.0,
                "plan_cache_hit_total": 0.0,
            },
        }

        rows = perf_comparator.build_source_sqlstat_delta_rows(
            start_snapshot,
            end_snapshot,
            captured_sql_ids={"sql-keep"},
            default_schema="APP",
            captured_at="2026-04-22T18:40:00Z",
        )

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["sql_id"], "sql-new")
        self.assertEqual(row["source_execution_count"], 4)
        self.assertAlmostEqual(row["source_total_elapsed_us"], 4000.0)
        self.assertEqual(row["source_ob_plan_type_raw"], "3")
        self.assertEqual(row["source_ob_is_executor_rpc"], "1")
        self.assertEqual(row["source_ob_is_hit_plan"], "0")

    def test_build_source_plan_cache_recent_rows_emits_missing_sql_ids(self):
        recent_rows = [
            {
                "sql_id": "sql-pc",
                "query_sql": "SELECT /* plan cache supplement */ * FROM invoices",
                "avg_exe_usec": 4200.0,
                "executions": 3.0,
                "elapsed_time": 12600.0,
                "buffer_gets": 300.0,
                "disk_reads": 12.0,
                "hit_count": 3.0,
                "type": "3",
                "table_scan": 1.0,
            }
        ]

        rows = perf_comparator.build_source_plan_cache_recent_rows(
            recent_rows,
            captured_sql_ids={"other-sql"},
            default_schema="APP",
            captured_at="2026-04-22T18:50:00Z",
        )

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["sql_id"], "sql-pc")
        self.assertEqual(row["source"], "ob_plan_cache_recent")
        self.assertEqual(row["source_execution_count"], 1)
        self.assertAlmostEqual(row["source_total_elapsed_us"], 4200.0)

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

    def test_probe_replay_capabilities_surfaces_optional_external_diagnostics(self):
        cfg = perf_comparator.AppConfig(
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
                "obclient_timeout": 5,
                "ob_session_query_timeout_us": 0,
                "ocp_ash_url_template": "https://ocp.local/api/ash?sql_id={sql_id}",
                "ocp_qpm_url_template": "https://ocp.local/api/qpm?sql_id={sql_id}",
                "ocp_auth_token_env": "PERF_OCP_TOKEN",
                "obdiag_executable": "/usr/local/bin/obdiag",
            },
            config_path="config.ini",
        )

        with mock.patch.object(
            perf_comparator,
            "obclient_run_sql",
            side_effect=[(True, "1\n", ""), (True, "ON\n", "")],
        ), mock.patch.dict(os.environ, {"PERF_OCP_TOKEN": "token"}, clear=False), mock.patch.object(
            perf_comparator,
            "_probe_plsql_profiler_capability",
            return_value={"available": True, "status": "ready"},
        ), mock.patch.object(
            perf_comparator.Path,
            "exists",
            return_value=True,
        ):
            result = perf_comparator.probe_replay_capabilities(cfg)

        self.assertTrue(result["sql_audit"])
        self.assertEqual(result["plsql_profiler"]["status"], "ready")
        self.assertEqual(result["ocp"]["status"], "ready")
        self.assertEqual(result["obdiag"]["status"], "ready")

    def test_probe_replay_capabilities_surfaces_native_ocp_mode(self):
        cfg = perf_comparator.AppConfig(
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
                "obclient_timeout": 5,
                "ob_session_query_timeout_us": 0,
                "ocp_base_url": "https://ocp.tidba.com:3600",
                "ocp_authorization_env": "PERF_OCP_AUTH",
                "ocp_cluster_id": "8",
                "ocp_tenant_id": "19",
                "ocp_verify_tls": False,
                "ocp_window_minutes": 30,
                "ocp_query_limit": 10,
            },
            config_path="config.ini",
        )

        with mock.patch.object(
            perf_comparator,
            "obclient_run_sql",
            side_effect=[(True, "1\n", ""), (True, "ON\n", "")],
        ), mock.patch.dict(os.environ, {"PERF_OCP_AUTH": "Basic xxx"}, clear=False), mock.patch.object(
            perf_comparator,
            "_probe_plsql_profiler_capability",
            return_value={"available": True, "status": "ready"},
        ), mock.patch.object(
            perf_comparator.Path,
            "exists",
            return_value=True,
        ):
            result = perf_comparator.probe_replay_capabilities(cfg)

        self.assertEqual(result["ocp"]["status"], "ready")
        self.assertEqual(result["ocp"]["mode"], "native")

    def test_build_ocp_headers_generates_basic_auth_from_username_and_password(self):
        cfg = perf_comparator.AppConfig(
            oracle_source={},
            oceanbase_source={},
            oceanbase_target={},
            settings={
                "ocp_username": "admin",
                "ocp_password": "PAssw0rd01##",
            },
            config_path="config.ini",
        )

        headers = perf_comparator._build_ocp_headers(cfg)

        self.assertEqual(
            headers["Authorization"], "Basic YWRtaW46UEFzc3cwcmQwMSMj"
        )

    def test_resolve_ocp_target_ids_uses_cluster_and_tenant_names(self):
        cfg = perf_comparator.AppConfig(
            oracle_source={},
            oceanbase_source={},
            oceanbase_target={},
            settings={
                "ocp_base_url": "https://ocp.tidba.com:3600",
                "ocp_username": "admin",
                "ocp_password": "PAssw0rd01##",
                "ocp_cluster_name": "observer147",
                "ocp_tenant_name": "ob4ora",
                "ocp_verify_tls": False,
                "ocp_timeout": 15,
            },
            config_path="config.ini",
        )

        with mock.patch.object(
            perf_comparator,
            "_fetch_ocp_cluster_inventory",
            return_value=[
                {
                    "id": 11,
                    "name": "observer147",
                    "tenants": [{"id": 35, "name": "sys"}, {"id": 36, "name": "ob4ora"}],
                }
            ],
        ):
            resolved = perf_comparator.resolve_ocp_target_ids(cfg)

        self.assertEqual(resolved["cluster_id"], "11")
        self.assertEqual(resolved["tenant_id"], "36")


class PerfComparatorReplayEvidenceTests(unittest.TestCase):
    def _build_config(self, verify_results=False, plsql_profile=False):
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
                "plsql_profile": plsql_profile,
                "plsql_profile_top_n": 10,
                "plsql_profile_source_context": 1,
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

    def test_replay_statement_attaches_plsql_profile_evidence(self):
        config = self._build_config(plsql_profile=True)
        workload_row = {
            "sql_id": "pkg-1",
            "sql_text": "BEGIN test_profiler_pkg.run_workload; END",
            "baseline_avg_elapsed_us": 1000.0,
            "oracle_avg_elapsed_us": 1000.0,
            "oracle_avg_logical_reads": 20.0,
        }
        backend = mock.Mock()
        backend.execute.return_value = (True, "ok", "")
        backend.explain.return_value = (False, "", "explain not supported")

        with mock.patch.object(
            perf_comparator,
            "_query_recent_audit_row",
            return_value={
                "ob_elapsed_us": 2200.0,
                "ob_net_time_us": 1500.0,
                "ob_plan_type_raw": "3",
                "ob_is_hit_plan": "1",
                "ob_is_executor_rpc": "1",
                "ob_logical_reads": 40.0,
                "trace_id": "trace-1",
            },
        ), mock.patch.object(
            perf_comparator,
            "collect_plsql_profile",
            return_value={
                "status": "ok",
                "runid": 42,
                "top_lines": [
                    {
                        "owner": "OMS_USER",
                        "unit_name": "TEST_PROFILER_PKG",
                        "unit_type": "PACKAGE BODY",
                        "line": 18,
                        "total_time_us": 900000.0,
                        "source_text": "FOR i IN 1..500000 LOOP",
                    }
                ],
                "artifact_path": "/tmp/plsql_profile.jsonl",
            },
        ) as profile_mock:
            replay_row = perf_comparator.replay_statement(config, workload_row, backend=backend)

        profile_mock.assert_called_once()
        self.assertEqual(replay_row["plsql_profile_status"], "ok")
        self.assertEqual(replay_row["plsql_profile_runid"], 42)
        self.assertIn("TEST_PROFILER_PKG", replay_row["plsql_profile_summary"])

    def test_collect_plsql_profile_initializes_profiler_objects_once(self):
        config = self._build_config(plsql_profile=True)
        config.settings["_current_run_id"] = "20260422_200000"
        config.settings["_plsql_profiler_init_status"] = None
        workload_row = {
            "sql_id": "pkg-1",
            "sql_text": "BEGIN test_profiler_pkg.run_workload; END",
        }

        responses = [
            (True, "", ""),
            (True, "", ""),
            (True, "42\n", ""),
            (True, "OMS_USER\tTEST_PROFILER_PKG\tPACKAGE BODY\t18\t10\t900000\tFOR i IN 1..500000 LOOP\n", ""),
            (True, "OMS_USER\tTEST_PROFILER_PKG\tPACKAGE BODY\t900000\t10\n", ""),
            (True, "", ""),
            (True, "43\n", ""),
            (True, "OMS_USER\tTEST_PROFILER_PKG\tPACKAGE BODY\t18\t10\t900000\tFOR i IN 1..500000 LOOP\n", ""),
            (True, "OMS_USER\tTEST_PROFILER_PKG\tPACKAGE BODY\t900000\t10\n", ""),
        ]

        with mock.patch.object(
            perf_comparator,
            "obclient_run_sql",
            side_effect=responses,
        ) as obclient_mock, mock.patch.object(
            perf_comparator,
            "_load_plsql_source_lines",
            return_value={
                "lines": {18: "FOR i IN 1..500000 LOOP"},
                "source_view": "DBA_SOURCE",
                "source_layout": "line_rows",
                "source_mapping_strategy": "dba_source_line_rows",
                "source_mapping_confidence": "high",
                "ob_version": "4.2.5.7",
            },
        ), mock.patch.object(
            perf_comparator,
            "_fetch_plsql_source_context",
            return_value=[],
        ):
            first = perf_comparator.collect_plsql_profile(
                config, workload_row, "BEGIN test_profiler_pkg.run_workload; END", 5
            )
            second = perf_comparator.collect_plsql_profile(
                config, workload_row, "BEGIN test_profiler_pkg.run_workload; END", 5
            )

        self.assertEqual(first["status"], "ok")
        self.assertEqual(second["status"], "ok")
        executed_sql = [call.args[1] for call in obclient_mock.call_args_list]
        init_calls = [
            sql for sql in executed_sql if "DBMS_PROFILER.OB_INIT_OBJECTS(FALSE)" in sql
        ]
        self.assertEqual(len(init_calls), 1)

    def test_collect_plsql_profile_retries_with_single_block_payload(self):
        config = self._build_config(plsql_profile=True)
        config.settings["_current_run_id"] = "20260422_200002"
        workload_row = {
            "sql_id": "pkg-1",
            "sql_text": "CALL TEST_PROFILER_PKG.run_profile_workload()",
        }

        responses = [
            (True, "", ""),
            (
                False,
                "",
                "ORA-00900: You have an error in your SQL syntax; check the manual that corresponds to your OceanBase version for the right syntax to use near 'BEGIN' at line 2",
            ),
            (True, "", ""),
            (True, "44\n", ""),
            (True, "OMS_USER\tTEST_PROFILER_PKG\tPACKAGE BODY\t18\t10\t900000\tFOR i IN 1..500000 LOOP\n", ""),
            (True, "OMS_USER\tTEST_PROFILER_PKG\tPACKAGE BODY\t900000\t10\n", ""),
        ]

        with mock.patch.object(
            perf_comparator,
            "obclient_run_sql",
            side_effect=responses,
        ) as obclient_mock, mock.patch.object(
            perf_comparator,
            "_load_plsql_source_lines",
            return_value={
                "lines": {18: "FOR i IN 1..500000 LOOP"},
                "source_view": "DBA_SOURCE",
                "source_layout": "line_rows",
                "source_mapping_strategy": "dba_source_line_rows",
                "source_mapping_confidence": "high",
                "ob_version": "4.2.5.7",
            },
        ), mock.patch.object(
            perf_comparator,
            "_fetch_plsql_source_context",
            return_value=[],
        ):
            result = perf_comparator.collect_plsql_profile(
                config, workload_row, "CALL TEST_PROFILER_PKG.run_profile_workload()", 30
            )

        self.assertEqual(result["status"], "ok")
        executed_sql = [call.args[1] for call in obclient_mock.call_args_list]
        self.assertEqual(len([sql for sql in executed_sql if "START_PROFILER" in sql]), 2)
        self.assertTrue(any("TEST_PROFILER_PKG.run_profile_workload" in sql and "STOP_PROFILER" in sql for sql in executed_sql))

    def test_get_oceanbase_version_caches_probe_result(self):
        config = self._build_config(plsql_profile=True)

        with mock.patch.object(
            perf_comparator,
            "obclient_run_sql",
            return_value=(True, "4.2.5.7\n", ""),
        ) as obclient_mock:
            first = perf_comparator.get_oceanbase_version(config)
            second = perf_comparator.get_oceanbase_version(config)

        self.assertEqual(first, "4.2.5.7")
        self.assertEqual(second, "4.2.5.7")
        self.assertEqual(obclient_mock.call_count, 1)

    def test_load_plsql_source_lines_reconstructs_single_row_dba_source_blob(self):
        config = self._build_config(plsql_profile=True)
        encoded_source = (
            "1\tCREATE OR REPLACE PACKAGE BODY TEST_PROFILER_PKG AS\x1f"
            "  PROCEDURE run_workload IS\x1f"
            "  BEGIN\x1f"
            "    FOR i IN 1..500000 LOOP\x1f"
            "      NULL;\x1f"
            "    END LOOP;\x1f"
            "  END;\x1f"
            "END;\n"
        )

        with mock.patch.object(
            perf_comparator,
            "obclient_run_sql",
            side_effect=[
                (True, "4.2.5.7\n", ""),
                (True, encoded_source, ""),
            ],
        ):
            source_info = perf_comparator._load_plsql_source_lines(
                config,
                "OMS_USER",
                "TEST_PROFILER_PKG",
                "PACKAGE BODY",
            )

        self.assertEqual(source_info["ob_version"], "4.2.5.7")
        self.assertEqual(source_info["source_view"], "DBA_SOURCE")
        self.assertEqual(source_info["source_layout"], "single_row_clob")
        self.assertEqual(source_info["source_mapping_strategy"], "dba_source_blob_split")
        self.assertEqual(source_info["source_mapping_confidence"], "medium")
        self.assertEqual(source_info["lines"][4], "    FOR i IN 1..500000 LOOP")

    def test_load_plsql_source_lines_falls_back_to_all_source_line_rows(self):
        config = self._build_config(plsql_profile=True)

        with mock.patch.object(
            perf_comparator,
            "obclient_run_sql",
            side_effect=[
                (True, "4.2.5.7\n", ""),
                (False, "", "ORA-00942: table or view does not exist"),
                (
                    True,
                    "17\tFOR i IN 1..500000 LOOP\n18\t  NULL;\n",
                    "",
                ),
            ],
        ):
            source_info = perf_comparator._load_plsql_source_lines(
                config,
                "OMS_USER",
                "TEST_PROFILER_PKG",
                "PACKAGE BODY",
            )

        self.assertEqual(source_info["source_view"], "ALL_SOURCE")
        self.assertEqual(source_info["source_layout"], "line_rows")
        self.assertEqual(source_info["source_mapping_confidence"], "high")
        self.assertEqual(source_info["lines"][17], "FOR i IN 1..500000 LOOP")
        self.assertEqual(source_info["lines"][18], "  NULL;")

    def test_collect_plsql_profile_skips_when_profiler_init_fails(self):
        config = self._build_config(plsql_profile=True)
        config.settings["_current_run_id"] = "20260422_200001"
        workload_row = {
            "sql_id": "pkg-1",
            "sql_text": "BEGIN test_profiler_pkg.run_workload; END",
        }

        with mock.patch.object(
            perf_comparator,
            "obclient_run_sql",
            return_value=(False, "", "init failed"),
        ) as obclient_mock:
            result = perf_comparator.collect_plsql_profile(
                config, workload_row, "BEGIN test_profiler_pkg.run_workload; END", 5
            )

        self.assertEqual(result["status"], "skipped")
        self.assertIn("init failed", result["error"])
        self.assertEqual(obclient_mock.call_count, 1)

    def test_collect_external_row_diagnostics_uses_native_ocp_sql_endpoints(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = self._build_config()
            config.settings.update(
                {
                    "report_dir": tmpdir,
                    "ocp_base_url": "https://ocp.tidba.com:3600",
                    "ocp_authorization_env": "PERF_OCP_AUTH",
                    "ocp_cluster_id": "8",
                    "ocp_tenant_id": "19",
                    "ocp_verify_tls": False,
                    "ocp_window_minutes": 30,
                    "ocp_query_limit": 10,
                }
            )
            row = {
                "sql_id": "sql-1",
                "sql_text": "SELECT * FROM orders WHERE status = 'ACTIVE'",
                "replayed_at": "2026-04-22T10:00:00Z",
                "ob_status": "ok",
                "ob_elapsed_us": 3000.0,
            }

            requested_urls = []

            class _FakeResponse(object):
                def __init__(self, payload):
                    self.payload = payload

                def read(self):
                    return self.payload.encode("utf-8")

            def _urlopen(request, timeout=None, context=None):
                url = request.full_url
                requested_urls.append(url)
                if "topSql" in url:
                    return _FakeResponse(
                        json.dumps(
                            {
                                "data": {
                                    "contents": [
                                        {
                                            "sqlId": "A1B2C3",
                                            "avgElapsedTime": 1234,
                                            "executions": 8,
                                            "sqlText": "SELECT * FROM orders WHERE status = 'ACTIVE'",
                                        }
                                    ]
                                }
                            }
                        )
                    )
                if "slowSql" in url:
                    return _FakeResponse(json.dumps({"data": {"contents": []}}))
                if "/sql/A1B2C3/text" in url:
                    return _FakeResponse(
                        json.dumps(
                            {"data": {"sqlText": "SELECT * FROM orders WHERE status = 'ACTIVE'"}}
                        )
                    )
                raise AssertionError("unexpected url: %s" % url)

            with mock.patch.dict(os.environ, {"PERF_OCP_AUTH": "Basic xxx"}, clear=False), mock.patch.object(
                perf_comparator.urllib.request,
                "urlopen",
                side_effect=_urlopen,
            ):
                result = perf_comparator.collect_external_row_diagnostics(config, row, "20260422_200100")

            self.assertEqual(result["ocp"]["status"], "ok")
            self.assertIn("A1B2C3", result["ocp"]["summary"])
            self.assertTrue(any("/api/v2/ob/clusters/8/tenants/19/topSql" in url for url in requested_urls))
            self.assertTrue(any("/api/v2/ob/clusters/8/tenants/19/slowSql" in url for url in requested_urls))
            self.assertTrue(any("/api/v2/ob/clusters/8/tenants/19/sql/A1B2C3/text" in url for url in requested_urls))

    def test_collect_external_row_diagnostics_fetches_native_ocp_trends_and_target_context(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = self._build_config()
            config.settings.update(
                {
                    "report_dir": tmpdir,
                    "ocp_base_url": "https://ocp.tidba.com:3600",
                    "ocp_username": "admin",
                    "ocp_password": "PAssw0rd01##",
                    "ocp_cluster_name": "observer147",
                    "ocp_tenant_name": "ob4ora",
                    "ocp_verify_tls": False,
                    "ocp_window_minutes": 30,
                    "ocp_query_limit": 10,
                }
            )
            row = {
                "sql_id": "sql-1",
                "sql_text": "SELECT * FROM orders WHERE status = 'ACTIVE'",
                "replayed_at": "2026-04-22T10:00:00Z",
                "ob_status": "ok",
                "ob_elapsed_us": 3000.0,
            }
            requested_urls = []

            class _FakeResponse(object):
                def __init__(self, payload):
                    self.payload = payload

                def read(self):
                    return self.payload.encode("utf-8")

            def _urlopen(request, timeout=None, context=None):
                url = request.full_url
                requested_urls.append(url)
                if url.endswith("/api/v2/ob/clusters"):
                    return _FakeResponse(
                        json.dumps(
                            {
                                "data": {
                                    "contents": [
                                        {
                                            "id": 11,
                                            "name": "observer147",
                                            "tenants": [{"id": 36, "name": "ob4ora"}],
                                        }
                                    ]
                                }
                            }
                        )
                    )
                if "topSql" in url:
                    return _FakeResponse(
                        json.dumps(
                            {"data": {"contents": [{"sqlId": "A1B2C3", "sqlText": row["sql_text"]}]}}
                        )
                    )
                if "slowSql" in url:
                    return _FakeResponse(json.dumps({"data": {"contents": []}}))
                if "/sql/A1B2C3/text" in url:
                    return _FakeResponse(json.dumps({"data": {"sqlText": row["sql_text"]}}))
                if "/sqls/A1B2C3/trends" in url:
                    return _FakeResponse(
                        json.dumps({"data": {"contents": [{"timestamp": "2026-04-22T10:00:00Z", "avgElapsedTime": 1234}]}})
                    )
                raise AssertionError("unexpected url: %s" % url)

            with mock.patch.object(
                perf_comparator.urllib.request,
                "urlopen",
                side_effect=_urlopen,
            ):
                result = perf_comparator.collect_external_row_diagnostics(config, row, "20260422_200101")

            self.assertEqual(result["ocp"]["status"], "ok")
            self.assertIn("cluster=11", result["ocp"]["summary"])
            self.assertIn("tenant=36", result["ocp"]["summary"])
            self.assertTrue(any("/api/v2/ob/clusters" in url for url in requested_urls))
            self.assertTrue(any("/sqls/A1B2C3/trends" in url for url in requested_urls))


class PerfComparatorRealDbValidationTests(unittest.TestCase):
    def test_run_realdb_verification_writes_summary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = perf_comparator.AppConfig(
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
                    "workloads_dir": tmpdir,
                    "report_dir": str(Path(tmpdir) / "reports"),
                    "verify_results": True,
                    "result_sample_limit": 10,
                    "obclient_timeout": 5,
                    "ob_session_query_timeout_us": 0,
                },
                config_path="config.ini",
            )
            args = argparse.Namespace(
                sql_file=None,
                realdb_oracle_config=None,
                realdb_ob_source_config=None,
                realdb_deploy_profile_package=False,
                realdb_profile_package_sql=None,
                realdb_profile_package_call=None,
                realdb_cleanup_profile_package=False,
            )

            with mock.patch.object(
                perf_comparator,
                "run_realdb_oracle_smoke",
                return_value={"step": "oracle_replay_smoke", "status": "passed"},
            ), mock.patch.object(
                perf_comparator,
                "run_realdb_ob_source_smoke",
                return_value={"step": "ob_source_capture_smoke", "status": "skipped"},
            ), mock.patch.object(
                perf_comparator,
                "run_realdb_profiler_smoke",
                return_value={"step": "plsql_profiler_smoke", "status": "skipped"},
            ):
                summary_path = perf_comparator.run_realdb_verification(config, args, "20260422_170000")

            summary = json.loads(Path(summary_path).read_text(encoding="utf-8"))
            self.assertEqual(summary["status"], "passed")
            self.assertEqual(len(summary["steps"]), 3)
            self.assertEqual(summary["steps"][0]["status"], "passed")


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

    def test_report_only_mode_surfaces_plan_risk_and_plsql_profile(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            workload_path = Path(tmpdir) / "workload_20260422_153000.jsonl"
            replay_path = Path(tmpdir) / "replay_20260422_153000.jsonl"
            report_dir = Path(tmpdir) / "reports"
            perf_comparator.append_jsonl(
                workload_path,
                {
                    "sql_id": "pkg-1",
                    "sql_text": "BEGIN test_profiler_pkg.run_workload; END",
                    "baseline_avg_elapsed_us": 1000.0,
                    "baseline_avg_logical_reads": 20.0,
                    "oracle_plan_rows": [
                        {"id": 1, "operation": "TABLE ACCESS", "options": "BY INDEX ROWID", "object_name": "ORDERS"}
                    ],
                },
            )
            perf_comparator.append_jsonl(
                replay_path,
                {
                    "sql_id": "pkg-1",
                    "sql_text": "BEGIN test_profiler_pkg.run_workload; END",
                    "ob_status": "ok",
                    "ob_elapsed_us": 3000.0,
                    "ob_net_time_us": 2100.0,
                    "ob_plan_type_raw": "3",
                    "ob_plan_rows": [{"id": 1, "operator": "TABLE LOOKUP", "name": "ORDERS"}],
                    "plan_diff_signals": [
                        {"signal_id": "LOOKUP-RISK", "severity": "high", "message": "lookup risk"}
                    ],
                    "plsql_profile_status": "ok",
                    "plsql_profile_summary": "TEST_PROFILER_PKG:18:FOR i IN 1..500000 LOOP",
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
            self.assertIn("plan-risk=LOOKUP-RISK", html_text)
            self.assertIn("plsql=TEST_PROFILER_PKG", html_text)

    def test_report_only_mode_surfaces_plsql_profile_mapping_confidence(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            workload_path = Path(tmpdir) / "workload_20260422_153500.jsonl"
            replay_path = Path(tmpdir) / "replay_20260422_153500.jsonl"
            report_dir = Path(tmpdir) / "reports"
            perf_comparator.append_jsonl(
                workload_path,
                {
                    "sql_id": "pkg-map-1",
                    "sql_text": "BEGIN test_profiler_pkg.run_workload; END",
                    "baseline_avg_elapsed_us": 1000.0,
                    "baseline_avg_logical_reads": 20.0,
                },
            )
            perf_comparator.append_jsonl(
                replay_path,
                {
                    "sql_id": "pkg-map-1",
                    "sql_text": "BEGIN test_profiler_pkg.run_workload; END",
                    "ob_status": "ok",
                    "ob_elapsed_us": 2600.0,
                    "ob_net_time_us": 1200.0,
                    "ob_plan_type_raw": "3",
                    "plsql_profile_status": "ok",
                    "plsql_profile_top_lines": [
                        {
                            "owner": "OMS_USER",
                            "unit_name": "TEST_PROFILER_PKG",
                            "unit_type": "PACKAGE BODY",
                            "line": 18,
                            "source_text": "FOR i IN 1..500000 LOOP",
                            "source_mapping_strategy": "dba_source_blob_split",
                            "source_mapping_confidence": "medium",
                            "source_view": "DBA_SOURCE",
                            "source_layout": "single_row_clob",
                            "ob_version": "4.2.5.7",
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
            summary_path = next(report_dir.glob("perf_report_*_summary.txt"))
            hints_path = next(report_dir.glob("perf_hints_*.sql"))
            html_text = html_path.read_text(encoding="utf-8")
            summary_text = summary_path.read_text(encoding="utf-8")
            hints_text = hints_path.read_text(encoding="utf-8")
            self.assertIn("plsql-map=medium@dba_source_blob_split", html_text)
            self.assertIn("plsql_map=medium@dba_source_blob_split", summary_text)
            self.assertIn("-- plsql-profile-map: medium@dba_source_blob_split", hints_text)

    def test_report_only_mode_surfaces_plsql_profile_diagnosis_summary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            workload_path = Path(tmpdir) / "workload_20260422_153700.jsonl"
            replay_path = Path(tmpdir) / "replay_20260422_153700.jsonl"
            report_dir = Path(tmpdir) / "reports"
            perf_comparator.append_jsonl(
                workload_path,
                {
                    "sql_id": "pkg-diag-1",
                    "sql_text": "BEGIN order_sync_pkg.run_batch; END",
                    "baseline_avg_elapsed_us": 1000.0,
                    "baseline_avg_logical_reads": 20.0,
                },
            )
            perf_comparator.append_jsonl(
                replay_path,
                {
                    "sql_id": "pkg-diag-1",
                    "sql_text": "BEGIN order_sync_pkg.run_batch; END",
                    "ob_status": "ok",
                    "ob_elapsed_us": 3800.0,
                    "ob_net_time_us": 2100.0,
                    "ob_plan_type_raw": "3",
                    "plsql_profile_status": "ok",
                    "plsql_profile_summary": "ORDER_SYNC_PKG:88:EXECUTE IMMEDIATE v_sql USING 'DONE', rec.order_id;",
                    "plsql_profile_diagnosis_summary": "ORDER_SYNC_PKG:87-90:dynamic_sql_in_loop",
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
            summary_path = next(report_dir.glob("perf_report_*_summary.txt"))
            hints_path = next(report_dir.glob("perf_hints_*.sql"))
            html_text = html_path.read_text(encoding="utf-8")
            summary_text = summary_path.read_text(encoding="utf-8")
            hints_text = hints_path.read_text(encoding="utf-8")
            self.assertIn("plsql_diag=ORDER_SYNC_PKG:87-90:dynamic_sql_in_loop", summary_text)
            self.assertIn("plsql-diag=ORDER_SYNC_PKG:87-90:dynamic_sql_in_loop", html_text)
            self.assertIn("-- plsql-profile-diagnosis: ORDER_SYNC_PKG:87-90:dynamic_sql_in_loop", hints_text)

    def test_report_only_mode_renders_chart_sections(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            workload_path = Path(tmpdir) / "workload_20260422_153000.jsonl"
            replay_path = Path(tmpdir) / "replay_20260422_153000.jsonl"
            report_dir = Path(tmpdir) / "reports"
            perf_comparator.append_jsonl(
                workload_path,
                [
                    {
                        "sql_id": "sql-fast",
                        "sql_text": "SELECT * FROM fast_table",
                        "baseline_avg_elapsed_us": 1000.0,
                        "baseline_avg_logical_reads": 20.0,
                        "oracle_plan_hash": "ora-1",
                    },
                    {
                        "sql_id": "sql-slow",
                        "sql_text": "SELECT * FROM slow_table",
                        "baseline_avg_elapsed_us": 1000.0,
                        "baseline_avg_logical_reads": 20.0,
                        "oracle_plan_hash": "ora-2",
                    },
                ],
            )
            perf_comparator.append_jsonl(
                replay_path,
                [
                    {
                        "sql_id": "sql-fast",
                        "sql_text": "SELECT * FROM fast_table",
                        "ob_status": "ok",
                        "ob_elapsed_us": 800.0,
                        "ob_logical_reads": 18.0,
                        "ob_net_time_us": 50.0,
                        "ob_plan_hash": "ob-1",
                    },
                    {
                        "sql_id": "sql-slow",
                        "sql_text": "SELECT * FROM slow_table",
                        "ob_status": "ok",
                        "ob_elapsed_us": 3000.0,
                        "ob_logical_reads": 60.0,
                        "ob_net_time_us": 2100.0,
                        "ob_plan_hash": "ob-2",
                    },
                ],
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
            self.assertIn('id="overview-charts"', html_text)
            self.assertIn('id="distribution-chart"', html_text)
            self.assertIn('id="timing-chart"', html_text)
            self.assertIn("<svg", html_text)

    def test_report_only_mode_writes_executable_hints_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            workload_path = Path(tmpdir) / "workload_20260422_153000.jsonl"
            replay_path = Path(tmpdir) / "replay_20260422_153000.jsonl"
            report_dir = Path(tmpdir) / "reports"
            perf_comparator.append_jsonl(
                workload_path,
                [
                    {
                        "sql_id": "sql-join-1",
                        "sql_text": "SELECT * FROM orders o JOIN order_items i ON i.order_id = o.id",
                        "baseline_avg_elapsed_us": 1000.0,
                        "baseline_avg_logical_reads": 20.0,
                    },
                    {
                        "sql_id": "pkg-1",
                        "sql_text": "BEGIN insurance_workload_pkg_small.run_profile_workload; END",
                        "baseline_avg_elapsed_us": 1000.0,
                        "baseline_avg_logical_reads": 20.0,
                    },
                ],
            )
            perf_comparator.append_jsonl(
                replay_path,
                [
                    {
                        "sql_id": "sql-join-1",
                        "sql_text": "SELECT * FROM orders o JOIN order_items i ON i.order_id = o.id",
                        "ob_status": "ok",
                        "ob_elapsed_us": 3000.0,
                        "ob_logical_reads": 60.0,
                        "ob_net_time_us": 2200.0,
                        "ob_get_plan_time_us": 700.0,
                        "ob_is_hit_plan": "0",
                        "ob_plan_hash": "ob-join-1",
                    },
                    {
                        "sql_id": "pkg-1",
                        "sql_text": "BEGIN insurance_workload_pkg_small.run_profile_workload; END",
                        "ob_status": "ok",
                        "ob_elapsed_us": 3000.0,
                        "ob_logical_reads": 10.0,
                        "ob_net_time_us": 2200.0,
                        "ob_is_executor_rpc": "1",
                        "plsql_profile_status": "ok",
                        "plsql_profile_top_lines": [
                            {
                                "owner": "OMS_USER",
                                "unit_name": "INSURANCE_WORKLOAD_PKG_SMALL",
                                "unit_type": "PACKAGE BODY",
                                "line": 87,
                                "total_time_us": 820000.0,
                                "source_text": "FOR i IN 1..v_ids.COUNT LOOP",
                                "context_lines": [
                                    {"line": 87, "text": "UPDATE orders SET status = 'DONE' WHERE id = v_ids(i);"},
                                ],
                            }
                        ],
                    },
                ],
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
            hints_path = next(report_dir.glob("perf_hints_*.sql"))
            hints_text = hints_path.read_text(encoding="utf-8")
            self.assertIn("CREATE TABLEGROUP", hints_text)
            self.assertIn("ALTER TABLE orders SET TABLEGROUP", hints_text)
            self.assertIn("CALL DBMS_XPLAN.ENABLE_OPT_TRACE()", hints_text)
            self.assertIn("FORALL", hints_text)
            self.assertIn("MERGE INTO orders", hints_text)

    def test_report_only_mode_collects_optional_external_diagnostics(self):
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
                    "ob_plan_type_raw": "3",
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
                    ocp_ash_url_template = https://ocp.local/api/ash?sql_id={{sql_id}}
                    obdiag_executable = /usr/local/bin/obdiag
                    """
                ).strip().format(workloads_dir=tmpdir, report_dir=report_dir)
                + "\n",
                encoding="utf-8",
            )

            with mock.patch.object(
                perf_comparator,
                "collect_external_row_diagnostics",
                return_value={
                    "ocp": {"status": "ok", "summary": "ash-hit", "artifact_path": "/tmp/ocp_sql-1.json"},
                    "obdiag": {"status": "ok", "summary": "bundle-ready", "artifact_path": "/tmp/obdiag_sql-1"},
                },
            ) as diagnostics_mock:
                exit_code = perf_comparator.main(
                    ["--mode", "report-only", "--config", str(config_path), "--replay", str(replay_path)]
                )

            self.assertEqual(exit_code, 0)
            diagnostics_mock.assert_called()
            html_path = next(report_dir.glob("perf_report_*.html"))
            html_text = html_path.read_text(encoding="utf-8")
            self.assertIn("ocp=ok:ash-hit", html_text)
            self.assertIn("obdiag=ok:bundle-ready", html_text)
            hints_path = next(report_dir.glob("perf_hints_*.sql"))
            hints_text = hints_path.read_text(encoding="utf-8")
            self.assertIn("external-diagnostics", hints_text)
            self.assertIn("ash-hit", hints_text)

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

    def test_verify_realdb_mode_generates_summary_artifact(self):
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
                    """
                ).strip().format(workloads_dir=workloads_dir, report_dir=report_dir)
                + "\n",
                encoding="utf-8",
            )

            with mock.patch.object(
                perf_comparator,
                "run_realdb_verification",
                return_value=Path(tmpdir) / "workloads" / "realdb_verify_20260422_170000.json",
            ) as verify_mock, mock.patch.object(
                perf_comparator,
                "validate_runtime_paths",
                return_value=perf_comparator.PreflightResult(),
            ):
                exit_code = perf_comparator.main(
                    ["--mode", "verify-realdb", "--config", str(config_path)]
                )

            self.assertEqual(exit_code, 0)
            verify_mock.assert_called_once()

    def test_source_report_mode_runs_with_only_ob_source(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.ini"
            workloads_dir = Path(tmpdir) / "workloads"
            report_dir = Path(tmpdir) / "reports"
            workload_path = workloads_dir / "workload_20260422_180000.jsonl"
            workloads_dir.mkdir(parents=True, exist_ok=True)
            perf_comparator.append_jsonl(
                workload_path,
                {
                    "sql_id": "sql-1",
                    "sql_text": "SELECT * FROM orders",
                    "sql_text_normalized": "SELECT * FROM ORDERS",
                    "baseline_avg_elapsed_us": 1200.0,
                    "oracle_avg_elapsed_us": 1200.0,
                    "oracle_avg_logical_reads": 90.0,
                    "oracle_avg_physical_reads": 4.0,
                    "source_ob_queue_time_us": 30.0,
                    "source_ob_get_plan_time_us": 20.0,
                    "source_ob_execute_time_us": 1150.0,
                    "source_ob_net_time_us": 800.0,
                    "source_ob_net_wait_time_us": 20.0,
                    "source_ob_plan_type_raw": "3",
                    "source_ob_is_hit_plan": "1",
                    "source_ob_is_executor_rpc": "1",
                    "source_ob_retry_cnt": 1.0,
                    "source_ob_memstore_read_rows": 300.0,
                    "source_ob_ssstore_read_rows": 100.0,
                    "source_ob_bloom_filter_filtered": 0.0,
                    "source_ob_trace_id": "trace-1",
                    "source_ob_request_id": "100",
                },
            )
            config_path.write_text(
                textwrap.dedent(
                    """
                    [OCEANBASE_SOURCE]
                    executable = /bin/echo
                    host = 127.0.0.1
                    port = 2881
                    user_string = root@test#obcluster
                    password = secret

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

            with mock.patch.object(
                perf_comparator,
                "capture_workload_from_ob_source",
                return_value=workload_path,
            ):
                exit_code = perf_comparator.main(
                    ["--mode", "source-report", "--config", str(config_path)]
                )

            self.assertEqual(exit_code, 0)
            summary_files = list(report_dir.glob("perf_report_*_summary.txt"))
            self.assertEqual(len(summary_files), 1)
            summary_text = summary_files[0].read_text(encoding="utf-8")
            self.assertIn("Report Mode: source-only", summary_text)
            self.assertIn("sql-1", summary_text)
            html_files = list(report_dir.glob("perf_report_*.html"))
            self.assertEqual(len(html_files), 1)
            html_text = html_files[0].read_text(encoding="utf-8")
            self.assertIn('id="overview-charts"', html_text)
            self.assertIn('id="source-distribution-chart"', html_text)
            self.assertIn('id="source-timing-chart"', html_text)

    def test_source_report_mode_surfaces_sql_text_source_distribution(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            workload_path = Path(tmpdir) / "workload_20260422_180000.jsonl"
            report_dir = Path(tmpdir) / "reports"
            perf_comparator.append_jsonl(
                workload_path,
                [
                    {
                        "sql_id": "sql-1",
                        "sql_text": "SELECT * FROM orders",
                        "sql_text_normalized": "SELECT * FROM ORDERS",
                        "baseline_avg_elapsed_us": 1200.0,
                        "oracle_avg_elapsed_us": 1200.0,
                        "oracle_avg_logical_reads": 90.0,
                        "source_ob_queue_time_us": 30.0,
                        "source_ob_get_plan_time_us": 20.0,
                        "source_ob_execute_time_us": 1150.0,
                        "source_ob_net_time_us": 800.0,
                        "source_ob_plan_type_raw": "3",
                        "source_ob_is_hit_plan": "1",
                        "source_ob_is_executor_rpc": "1",
                        "source_sql_text_source": "captured",
                    },
                    {
                        "sql_id": "sql-2",
                        "sql_text": "SELECT * FROM invoices",
                        "sql_text_normalized": "SELECT * FROM INVOICES",
                        "baseline_avg_elapsed_us": 1000.0,
                        "oracle_avg_elapsed_us": 1000.0,
                        "oracle_avg_logical_reads": 40.0,
                        "source_ob_queue_time_us": 20.0,
                        "source_ob_get_plan_time_us": 10.0,
                        "source_ob_execute_time_us": 900.0,
                        "source_ob_net_time_us": 100.0,
                        "source_ob_plan_type_raw": "1",
                        "source_ob_is_hit_plan": "1",
                        "source_ob_is_executor_rpc": "0",
                        "source_sql_text_source": "ocp_native",
                    },
                ],
            )
            config = perf_comparator.AppConfig(
                oracle_source={},
                oceanbase_source={
                    "executable": "/bin/echo",
                    "host": "127.0.0.1",
                    "port": "2881",
                    "user_string": "root@test#obcluster",
                    "password": "secret",
                },
                oceanbase_target={},
                settings={
                    "source_db_mode": "oceanbase",
                    "source_schemas": ["APP"],
                    "workloads_dir": tmpdir,
                    "report_dir": str(report_dir),
                    "top_n": 50,
                    "slowdown_threshold": 0.8,
                },
                config_path="config.ini",
            )

            report_paths = perf_comparator.generate_report_from_source_workload(
                config, workload_path, "20260422_200200"
            )

            summary_text = Path(report_paths["summary"]).read_text(encoding="utf-8")
            html_text = Path(report_paths["html"]).read_text(encoding="utf-8")
            hints_text = Path(report_paths["hints"]).read_text(encoding="utf-8")
            self.assertIn("SQL text recovery detail: local=0 ocp_native=1 ocp_template=0", summary_text)
            self.assertIn("sql: [captured]", summary_text)
            self.assertIn("sql: [ocp_native]", summary_text)
            self.assertIn('id="sql-source-chart"', html_text)
            self.assertIn("ocp_native", html_text)
            self.assertIn("-- sql_text_recovery_detail: local=0 ocp_native=1 ocp_template=0", hints_text)

    def test_source_report_surfaces_top_callers_and_separate_sql_plsql_sections(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            workload_path = Path(tmpdir) / "workload_20260422_181000.jsonl"
            report_dir = Path(tmpdir) / "reports"
            perf_comparator.append_jsonl(
                workload_path,
                [
                    {
                        "sql_id": "sql-caller-1",
                        "sql_text": "SELECT * FROM orders WHERE status = 'NEW'",
                        "sql_text_normalized": "SELECT * FROM ORDERS WHERE STATUS = 'NEW'",
                        "baseline_avg_elapsed_us": 1800.0,
                        "oracle_avg_elapsed_us": 1800.0,
                        "oracle_avg_logical_reads": 90.0,
                        "source_ob_queue_time_us": 30.0,
                        "source_ob_get_plan_time_us": 20.0,
                        "source_ob_execute_time_us": 1750.0,
                        "source_ob_net_time_us": 1000.0,
                        "source_ob_plan_type_raw": "3",
                        "source_ob_is_hit_plan": "1",
                        "source_ob_is_executor_rpc": "0",
                        "source_sql_text_source": "captured",
                        "source_tenant_name": "ob4ora",
                        "source_db_name": "observer147",
                        "source_user_name": "QA_ORDERS",
                        "source_user_client_ip": "172.16.1.10",
                    },
                    {
                        "sql_id": "pkg-caller-1",
                        "sql_text": "BEGIN settlement_pkg.run_close_day; END",
                        "sql_text_normalized": "BEGIN SETTLEMENT_PKG.RUN_CLOSE_DAY; END",
                        "baseline_avg_elapsed_us": 5200.0,
                        "oracle_avg_elapsed_us": 5200.0,
                        "oracle_avg_logical_reads": 40.0,
                        "source_ob_queue_time_us": 200.0,
                        "source_ob_get_plan_time_us": 60.0,
                        "source_ob_execute_time_us": 4940.0,
                        "source_ob_net_time_us": 2600.0,
                        "source_ob_plan_type_raw": "3",
                        "source_ob_is_hit_plan": "1",
                        "source_ob_is_executor_rpc": "1",
                        "source_sql_text_source": "captured",
                        "source_tenant_name": "ob4ora",
                        "source_db_name": "observer147",
                        "source_user_name": "QA_FINANCE",
                        "source_user_client_ip": "172.16.2.20",
                        "plsql_profile_status": "ok",
                        "plsql_profile_diagnosis_summary": "SETTLEMENT_PKG:88-95:row_by_row_sql_in_loop",
                    },
                ],
            )
            config = perf_comparator.AppConfig(
                oracle_source={},
                oceanbase_source={
                    "executable": "/bin/echo",
                    "host": "127.0.0.1",
                    "port": "2881",
                    "user_string": "root@test#obcluster",
                    "password": "secret",
                },
                oceanbase_target={},
                settings={
                    "source_db_mode": "oceanbase",
                    "source_schemas": ["APP"],
                    "workloads_dir": tmpdir,
                    "report_dir": str(report_dir),
                    "top_n": 50,
                    "slowdown_threshold": 0.8,
                    "source_actor_fields": ["tenant_name", "db_name", "user_name", "user_client_ip"],
                },
                config_path="config.ini",
            )

            report_paths = perf_comparator.generate_report_from_source_workload(
                config, workload_path, "20260422_201000"
            )

            summary_text = Path(report_paths["summary"]).read_text(encoding="utf-8")
            html_text = Path(report_paths["html"]).read_text(encoding="utf-8")
            hints_text = Path(report_paths["hints"]).read_text(encoding="utf-8")
            self.assertIn("Top caller groups:", summary_text)
            self.assertIn("Top slow SQL:", summary_text)
            self.assertIn("Top slow PL/SQL:", summary_text)
            self.assertIn("QA_FINANCE", summary_text)
            self.assertIn("SETTLEMENT_PKG:88-95:row_by_row_sql_in_loop", summary_text)
            self.assertIn('id="top-caller-groups"', html_text)
            self.assertIn('id="slow-plsql-section"', html_text)
            self.assertIn("-- top_callers:", hints_text)
            self.assertIn("-- slow_plsql:", hints_text)


class PerfComparatorDocumentationTests(unittest.TestCase):
    def test_readme_exists_and_mentions_runtime_modes(self):
        readme_path = Path(__file__).parent / "README.md"
        self.assertTrue(readme_path.exists())
        readme_text = readme_path.read_text(encoding="utf-8")
        self.assertIn("Python 3.7", readme_text)
        self.assertIn("batch", readme_text)
        self.assertIn("source-report", readme_text)
        self.assertIn("report-only", readme_text)

    def test_generate_source_report_summary_mentions_sql_text_coverage_gap(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            workload_path = Path(tmpdir) / "workload_20260422_180000.jsonl"
            report_dir = Path(tmpdir) / "reports"
            perf_comparator.append_jsonl(
                workload_path,
                {
                    "sql_id": "sql-1",
                    "sql_text": "NULL",
                    "sql_text_normalized": "NULL",
                    "baseline_avg_elapsed_us": 1200.0,
                    "oracle_avg_elapsed_us": 1200.0,
                    "oracle_avg_logical_reads": 90.0,
                    "oracle_avg_physical_reads": 4.0,
                    "source_ob_queue_time_us": 30.0,
                    "source_ob_get_plan_time_us": 20.0,
                    "source_ob_execute_time_us": 1150.0,
                    "source_ob_net_time_us": 800.0,
                    "source_ob_net_wait_time_us": 20.0,
                    "source_ob_plan_type_raw": "3",
                    "source_ob_is_hit_plan": "1",
                    "source_ob_is_executor_rpc": "1",
                    "source_ob_retry_cnt": 1.0,
                    "source_ob_memstore_read_rows": 300.0,
                    "source_ob_ssstore_read_rows": 100.0,
                    "source_ob_trace_id": "trace-1",
                    "source_ob_request_id": "100",
                },
            )
            config = perf_comparator.AppConfig(
                oracle_source={},
                oceanbase_source={
                    "executable": "/bin/echo",
                    "host": "127.0.0.2",
                    "port": "2883",
                    "user_string": "app@test#obcluster",
                    "password": "source_secret",
                },
                oceanbase_target={},
                settings={
                    "source_db_mode": "oceanbase",
                    "source_schemas": ["APP"],
                    "report_dir": str(report_dir),
                    "slowdown_threshold": 0.8,
                    "top_n": 20,
                },
                config_path="config.ini",
                oceanbase_source_sys={
                    "executable": "/bin/echo",
                    "host": "127.0.0.2",
                    "port": "2883",
                    "user_string": "SYS@ob4ora#observer147",
                    "password": "sys_secret",
                },
            )

            with mock.patch.object(
                perf_comparator,
                "lookup_source_sql_texts",
                return_value={},
            ), mock.patch.object(
                perf_comparator,
                "collect_source_plan_monitor_rows",
                return_value=[],
            ):
                report_paths = perf_comparator.generate_report_from_source_workload(
                    config, workload_path, "20260422_180000"
                )

            summary_text = report_paths["summary"].read_text(encoding="utf-8")
            self.assertIn("SQL text coverage:", summary_text)
            self.assertIn("OCEANBASE_SOURCE_SYS", summary_text)
            self.assertIn("_enable_sql_audit_query_sql", summary_text)

    def test_generate_source_report_skips_internal_perf_queries(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            workload_path = Path(tmpdir) / "workload_20260422_180100.jsonl"
            report_dir = Path(tmpdir) / "reports"
            perf_comparator.append_jsonl(
                workload_path,
                [
                    {
                        "sql_id": "internal-1",
                        "sql_text": "NULL",
                        "sql_text_normalized": "NULL",
                        "baseline_avg_elapsed_us": 5000.0,
                        "oracle_avg_elapsed_us": 5000.0,
                        "oracle_avg_logical_reads": 10.0,
                        "oracle_avg_physical_reads": 1.0,
                        "source_ob_queue_time_us": 0.0,
                        "source_ob_get_plan_time_us": 0.0,
                        "source_ob_execute_time_us": 5000.0,
                        "source_ob_net_time_us": 0.0,
                        "source_ob_net_wait_time_us": 0.0,
                        "source_ob_plan_type_raw": "1",
                        "source_ob_is_hit_plan": "1",
                        "source_ob_is_executor_rpc": "0",
                        "source_ob_retry_cnt": 0.0,
                        "source_ob_memstore_read_rows": 0.0,
                        "source_ob_ssstore_read_rows": 0.0,
                    },
                    {
                        "sql_id": "user-1",
                        "sql_text": "NULL",
                        "sql_text_normalized": "NULL",
                        "baseline_avg_elapsed_us": 1000.0,
                        "oracle_avg_elapsed_us": 1000.0,
                        "oracle_avg_logical_reads": 10.0,
                        "oracle_avg_physical_reads": 1.0,
                        "source_ob_queue_time_us": 0.0,
                        "source_ob_get_plan_time_us": 0.0,
                        "source_ob_execute_time_us": 1000.0,
                        "source_ob_net_time_us": 0.0,
                        "source_ob_net_wait_time_us": 0.0,
                        "source_ob_plan_type_raw": "1",
                        "source_ob_is_hit_plan": "1",
                        "source_ob_is_executor_rpc": "0",
                        "source_ob_retry_cnt": 0.0,
                        "source_ob_memstore_read_rows": 0.0,
                        "source_ob_ssstore_read_rows": 0.0,
                    },
                ],
            )
            config = perf_comparator.AppConfig(
                oracle_source={},
                oceanbase_source={
                    "executable": "/bin/echo",
                    "host": "127.0.0.2",
                    "port": "2883",
                    "user_string": "app@test#obcluster",
                    "password": "source_secret",
                },
                oceanbase_target={},
                settings={
                    "source_db_mode": "oceanbase",
                    "source_schemas": ["APP"],
                    "report_dir": str(report_dir),
                    "slowdown_threshold": 0.8,
                    "top_n": 20,
                },
                config_path="config.ini",
                oceanbase_source_sys={
                    "executable": "/bin/echo",
                    "host": "127.0.0.2",
                    "port": "2883",
                    "user_string": "SYS@ob4ora#observer147",
                    "password": "sys_secret",
                },
            )

            with mock.patch.object(
                perf_comparator,
                "lookup_source_sql_texts",
                return_value={
                    "internal-1": "SELECT /* perf_comparator_source_poll */ * FROM GV$OB_SQL_AUDIT",
                    "user-1": "SELECT /* user workload */ * FROM orders",
                },
            ), mock.patch.object(
                perf_comparator,
                "collect_source_plan_monitor_rows",
                return_value=[],
            ):
                report_paths = perf_comparator.generate_report_from_source_workload(
                    config, workload_path, "20260422_180100"
                )

            summary_text = report_paths["summary"].read_text(encoding="utf-8")
            self.assertIn("user workload", summary_text)
            self.assertNotIn("perf_comparator_source_poll", summary_text)


if __name__ == "__main__":
    unittest.main()
