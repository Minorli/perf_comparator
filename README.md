# perf_comparator

`perf_comparator` 是一个面向 Oracle -> OceanBase 迁移场景的单文件性能分析工具。它负责捕获工作负载、在 OceanBase 上回放、关联诊断证据，并产出可执行的调优建议与报告。

## 运行约束

- Python 3.7 兼容
- 运行时主程序保持为一个文件：`perf_comparator.py`
- 适合内网和生产交付环境，不依赖 Docker、Kafka 或额外服务
- 对可选能力一律做 capability probing 和 graceful degradation

## 当前模式

- `batch`
  - Oracle 捕获 -> OB 回放 -> 报告
  - 也支持 `--sql-file` 和 `--wcr-path`
- `stream`
  - Oracle `V$SQL` 增量追踪
- `replay-only`
  - 直接消费已有 workload JSONL，生成 replay JSONL
- `report-only`
  - 直接消费已有 replay JSONL，生成 HTML、TXT、SQL hints
- `source-report`
  - 只连接一个 OceanBase，捕获负载并生成排障报告，不和 Oracle 对比
- `check-config`
  - 校验配置和本地运行时依赖
- `verify-realdb`
  - 做实库冒烟验证，包括 Oracle 回放、PL/SQL profiler、OB source 捕获链路

## 关键能力

- Oracle 捕获源优先级：`AWR -> V$SQL -> Unified Audit -> WCR -> SQL file`
- OceanBase 诊断：`EXPLAIN`、`GV$OB_SQL_AUDIT`、`GV$SQL_PLAN_MONITOR`
- PL/SQL 行级剖析：`DBMS_PROFILER`
  - 首次使用会自动执行 `DBMS_PROFILER.OB_INIT_OBJECTS(FALSE)`
- 可执行调优建议：
  - `DIST-JOIN`
  - `PLAN-MISS`
  - `PLSQL-RPC`
- 报告输出：
  - `perf_report_<ts>.html`
  - `perf_report_<ts>_summary.txt`
  - `perf_hints_<ts>.sql`
- 可选外部诊断：
  - OCP URL template 拉取
  - obdiag CLI 采集

## 快速开始

### 1. 安装依赖

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. 准备配置

```bash
cp config.ini.template config.ini
```

至少需要：

- `[ORACLE_SOURCE]`
- `[OCEANBASE_TARGET]`
- `[SETTINGS].source_schemas`

如果是 `source-report`：

- `[SETTINGS].source_db_mode = oceanbase`
- `[OCEANBASE_SOURCE]`
- 建议配置 `[OCEANBASE_SOURCE_SYS]`，方便在 OB 4.2.5 上回填 `QUERY_SQL`

### 3. 运行

Oracle -> OB 全链路：

```bash
python3 perf_comparator.py --mode batch --config config.ini
```

从 SQL 文件回放：

```bash
python3 perf_comparator.py --mode batch --config config.ini --sql-file input.sql
```

从 WCR 导入：

```bash
python3 perf_comparator.py --mode batch --config config.ini --wcr-path workloads/sample.wcr
```

仅做报告：

```bash
python3 perf_comparator.py --mode report-only --config config.ini --replay workloads/replay_<ts>.jsonl
```

只看单个 OB 的负载问题：

```bash
python3 perf_comparator.py --mode source-report --config config.ini --duration 86400 --rolling-report-interval 300
```

这个模式适合后台连续跑一天，报告会按固定周期刷新同一组 HTML/TXT/SQL 文件，持续给出：

- Top caller groups
- Top slow SQL
- Top slow PL/SQL
- 每条热点的可能原因

## 关键配置项

基础配置：

- `source_schemas`
- `workloads_dir`
- `report_dir`
- `top_n`
- `hours`
- `slowdown_threshold`
- `plsql_profile`
- `rolling_report_interval`
- `source_actor_fields`

可选外部诊断：

- native OCP：
  - `ocp_base_url`
  - `ocp_authorization_env`
  - `ocp_username`
  - `ocp_password`
  - `ocp_password_env`
  - `ocp_cluster_id`
  - `ocp_tenant_id`
  - `ocp_cluster_name`
  - `ocp_tenant_name`
  - `ocp_verify_tls`
  - `ocp_window_minutes`
  - `ocp_query_limit`
- `ocp_ash_url_template`
- `ocp_qpm_url_template`
- `ocp_auth_token_env`
- `ocp_timeout`
- `obdiag_executable`
- `obdiag_extra_args`
- `obdiag_timeout`

说明：

- 如果你使用的是标准 OCP `api/v2`，优先用 native OCP 配置，不需要手工拼 SQL endpoint
- 认证优先级：
  - `ocp_authorization_env`
  - `ocp_username + ocp_password_env`
  - `ocp_username + ocp_password`
- 使用 `ocp_username + ocp_password(_env)` 时，程序会自动生成：
  - `Authorization: Basic <base64(username:password)>`
- 如果不想手填 `cluster_id/tenant_id`，可以改配：
  - `ocp_cluster_name`
  - `ocp_tenant_name`
  程序会通过 `/api/v2/ob/clusters` 自动解析
- 如果当前 OCP 只能通过 `curl -k` 访问，就把 `ocp_verify_tls = false`
- OCP 集成优先走 native `api/v2` SQL endpoints，特殊环境仍可回退到 URL template 模式
- native OCP 已接入：
  - `topSql`
  - `slowSql`
  - `sql/{sqlId}/text`
  - `sqls/{sqlId}/trends`
- obdiag 是可选能力，失败只会记到报告，不会阻塞主流程

## Source-Only SQL 获取协同

当 `source-report` 模式下普通用户看不到 `QUERY_SQL` 时，程序会按下面顺序补 SQL 文本：

1. 直接使用源端采集到的 `QUERY_SQL`
2. 使用 `[OCEANBASE_SOURCE_SYS]` 从 `GV$OB_SQLSTAT / GV$OB_PLAN_CACHE_PLAN_STAT / GV$OB_SQL_AUDIT` 回填
3. 使用 native OCP `sql/{sqlId}/text`
4. 使用模板式 OCP fallback

报告里会标出每条 SQL 最终来自哪条链路。
source-only HTML 里还会给出 SQL 来源分布图，帮助你判断当前环境究竟多依赖 OCP 还是本地视图回填。

## 24 小时后台抓取建议

推荐命令：

```bash
python3 perf_comparator.py --mode source-report --config config.ini --duration 86400 --rolling-report-interval 300
```

推荐配置：

- `source_db_mode = oceanbase`
- 配置 `[OCEANBASE_SOURCE]`
- 配置 `[OCEANBASE_SOURCE_SYS]`，用于 OB 4.2.5 上回填 SQL 文本
- `plsql_profile = true`，用于需要时抓取复杂包的行级热点
- `source_actor_fields = tenant_name,db_name,user_name,user_client_ip`

这样在多个测试部门同时压测时，最终报告会按 caller group 归类慢 SQL 和慢 PL/SQL，并尽量给出 RPC、分布式计划、热点代码块等原因。

## 产物

`workloads/` 下常见文件：

- `workload_<ts>.jsonl`
- `replay_<ts>.jsonl`
- `audit_dump_<ts>.jsonl`
- `capture_capability_<ts>.json`
- `replay_capability_<ts>.json`
- `plsql_profile_<ts>.jsonl`

`reports/` 下常见文件：

- `perf_report_<ts>.html`
- `perf_report_<ts>_summary.txt`
- `perf_hints_<ts>.sql`
- `external_diag_<run_id>_<sql_id>_*.txt`

## 验证

```bash
python3 -m py_compile perf_comparator.py test_perf_comparator.py
python3 -m unittest -v
openspec validate --type change add-rolling-ob-source-diagnostics --strict --no-interactive
```

## 参考文档

- [docs/runtime_setup.txt](docs/runtime_setup.txt)
- [openspec/project.md](openspec/project.md)
- [config.ini.template](config.ini.template)
