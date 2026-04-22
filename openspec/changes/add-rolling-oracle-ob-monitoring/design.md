## Context

生产使用方式分成两类：

1. 纯 OceanBase 源端排障
2. Oracle 源端业务压测，工具在后台持续捕获 Oracle 工作负载并回放到 OceanBase

第一类现在已有滚动 source-only 报告。第二类还缺一条完整的滚动链路。

## Goals

- `stream` 模式在长时间运行时持续捕获 Oracle 新工作负载
- 对新增 SQL / PL/SQL 指纹即时回放到 OceanBase
- 在固定间隔内刷新同一组 HTML/TXT/SQL 报告文件
- 对 OceanBase 源端 `QUERY_SQL` 隐藏风险做启动期和报告期双重提示

## Non-Goals

- 不追求精确重放 Oracle 每一次执行调用
- 不引入 Web UI、数据库或消息队列
- 不改变当前 `batch`、`report-only`、`replay-only` 的单次执行行为

## Decisions

### 1. 直接增强 `stream`，不新增新的主模式

`stream` 本身已经是 Oracle 增量捕获模式。增强它的编排比新增 `monitor` 模式更稳妥：

- 老的 CLI 入口不变
- 运维无需学习新模式
- 可以自然复用现有的 `replay_statement` 和 `generate_report_from_replay`

### 2. 滚动模式按“新指纹”回放，而不是尝试重放每次重复执行

Oracle `V$SQL` 更适合发现新的 SQL / PL/SQL 指纹，而不是精确枚举每一次调用。
因此滚动模式会：

- 增量抓取新的 `V$SQL` 行
- 以 `schema + sql_id + normalized_sql` 作为去重指纹
- 对首次发现的指纹执行回放并纳入报告

这样可以：

- 不遗漏新的调用类型
- 避免长时间运行时重复回放同一 SQL 导致成本失控
- 保持现有 replay 报告结构稳定

### 3. 滚动报告沿用同一组 run-scoped 文件路径

与 source-only 滚动报告一致，Oracle -> OB 滚动模式刷新：

- `perf_report_<run_id>.html`
- `perf_report_<run_id>_summary.txt`
- `perf_hints_<run_id>.sql`

这样运维可以在整个 24 小时窗口内始终查看同一组路径。

### 4. 采集上限与报告 Top N 解耦

`top_n` 是展示维度，不应该直接限制长跑采集面。
新增单独的 `capture_top_n`：

- Oracle 捕获查询使用 `capture_top_n`
- 报告仍使用 `top_n`

### 5. 非 SYS 源端登录的 `QUERY_SQL` 风险必须前置提醒

当 `source_db_mode = oceanbase` 且 `[OCEANBASE_SOURCE]` 不是 `SYS` 登录时：

- preflight 阶段产生显著 warning
- 主流程启动时再次输出 banner warning
- source-only 报告头部保留 warning block

提示内容必须明确包含：

- 使用 `SYS` 登录
- 或配置 `[OCEANBASE_SOURCE_SYS]`
- 如仍不可见，检查 `_enable_sql_audit_query_sql=true`

## Risks

- 滚动回放仍会增加目标端压力
  - 通过现有 `interval` 和 `rolling_report_interval` 控制节奏
- Oracle `V$SQL` 不是逐执行明细源
  - 方案定义为“不遗漏新的 SQL / PL/SQL 指纹”，不是逐调用审计
- 长时间运行的报告刷新可能触发额外外部诊断开销
  - 滚动报告跳过外部诊断扩展，结束时再生成完整最终报告
