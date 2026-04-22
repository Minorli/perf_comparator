# perf_comparator 设计文档

**项目**：Oracle → OceanBase SQL/PL/SQL 性能迁移专家系统  
**版本**：v1.0  
**日期**：2026-04-22  
**状态**：待实现

---

## 1. 背景与目标

Oracle 到 OceanBase 迁移后，SQL/PL/SQL 性能回退的根源在于底层架构的范式转变：Oracle 采用共享存储（Shared-Everything），OceanBase 采用无共享（Shared-Nothing）+ LSM-Tree 分布式架构。这种差异导致执行计划形态、网络开销、存储读写路径均发生根本变化，现有工具链无法自动识别和诊断跨平台性能差异。

**目标**：构建一套自动化的性能对比专家系统，实现：

1. **慢查询差异报告**：识别 Oracle 快但 OB 慢的 SQL，附执行计划对比
2. **优化建议**：针对每条慢 SQL 给出可操作的 OB 调优指令
3. **结果集验证**（后续迭代）：验证 Oracle 与 OB 查询结果集一致性

**约束**：
- Python 3.7 兼容，不引入重型依赖（无 Kafka、无 Docker）
- 客户内网可运行，不修改应用层和网络拓扑
- 与 `~/comparator` 项目解耦，作为独立工具

---

## 2. 整体架构

### 2.1 项目结构

```
/home/minorli/perf_comparator/
├── perf_comparator.py          # 主入口 CLI（串联三个阶段）
├── sql_capture.py              # 阶段一：Oracle SQL 捕获
├── sql_replay.py               # 阶段二：OB 回放 + 指标采集
├── sql_audit_daemon.py         # GV$OB_SQL_AUDIT 持续轮询守护进程
├── perf_report.py              # 阶段三：差异分析 + 报告生成
├── rules/
│   ├── __init__.py
│   ├── optimization_rules.py  # 六大架构级专家规则引擎
│   └── plan_operator_map.py   # Oracle ↔ OB 算子翻译矩阵
├── config.ini.template         # 配置模板
├── requirements.txt            # oracledb
└── workloads/                  # 中间文件目录（gitignore）
    ├── workload_<ts>.jsonl     # 捕获结果
    ├── replay_<ts>.jsonl       # 回放结果
    ├── audit_dump_<ts>.jsonl   # SQL Audit 持久化转储
    └── plsql_profile_<ts>.jsonl # PL/SQL 行级剖析（OB >= 4.2.3）
```

### 2.2 数据流

```
Oracle AWR / V$SQL / AuditTrail / WCR / SQL 文本
        ↓  sql_capture.py（自动探测能力，降级兜底）
workload_<ts>.jsonl
  { sql_id, sql_text, bind_vars, oracle_elapsed_us, oracle_plan, ... }
        ↓  sql_replay.py + sql_audit_daemon.py（并发）
replay_<ts>.jsonl
  { sql_id, ob_elapsed_us, ob_plan_type, ob_queue_time_us,
    ob_net_time_us, ob_is_executor_rpc, speedup_ratio, ... }
        ↓  perf_report.py
perf_report_<ts>.html           # 主报告（浏览器）
perf_report_<ts>_summary.txt    # 纯文本摘要
perf_hints_<ts>.sql             # 可直接执行的优化 SQL/DDL 片段
```

### 2.3 运行模式

| 模式 | 命令 | 说明 |
|------|------|------|
| `batch` | `--mode batch` | 一次性导出历史 SQL，离线回放（对应 OMA Full Performance Assessment）|
| `stream` | `--mode stream --interval 60` | 每 N 秒轮询 Oracle V$SQL 增量，持续追加 workload |
| `replay-only` | `--mode replay-only --workload <file>` | 跳过捕获，直接回放已有 workload |
| `report-only` | `--mode report-only --replay <file>` | 跳过捕获和回放，重新生成报告（调整规则后用）|

---

## 3. 阶段一：捕获层（sql_capture.py）

### 3.1 Oracle 能力自动探测顺序

```
1. AWR（DBA_HIST_SQLSTAT / DBA_HIST_SQL_PLAN）
   → 需要 Diagnostics Pack License
   → 探测：SELECT COUNT(*) FROM DBA_HIST_SQLSTAT WHERE ROWNUM=1
   → 优先级最高，含历史快照

2. V$SQL / V$SQL_PLAN
   → 无需额外 License，内存缓存
   → 探测：SELECT COUNT(*) FROM V$SQL WHERE ROWNUM=1
   → stream 模式主要数据源（按 LAST_ACTIVE_TIME 增量）

3. Oracle Unified Auditing / AUD$
   → 需要审计开启
   → 探测：SELECT COUNT(*) FROM UNIFIED_AUDIT_TRAIL WHERE ROWNUM=1
   → 含完整 SQL 文本和部分绑定变量

4. WCR 文件（本地路径）
   → --wcr-path 参数指定
   → 用户提前用 DBMS_WORKLOAD_CAPTURE 采集

5. 手动 SQL 文本文件（.txt/.sql）
   → --sql-file 参数指定，分号或 $$ 分隔
   → 最低公分母兜底，对齐 OMA TEXT 文件输入
```

探测结果写入 `capture_capability_<ts>.json`，供后续阶段参考。

### 3.2 workload JSONL schema

每行一条 SQL 事件：

```json
{
  "sql_id": "8f2a1c3d9e4b0f17",
  "sql_text": "SELECT * FROM orders WHERE status = :1",
  "sql_text_normalized": "SELECT * FROM orders WHERE status = :B1",
  "bind_vars": {"1": "ACTIVE"},
  "schema": "GBSJOB",
  "source": "awr",
  "captured_at": "2026-04-22T10:00:00",
  "oracle_executions": 1523,
  "oracle_avg_elapsed_us": 48200,
  "oracle_avg_cpu_us": 12300,
  "oracle_avg_logical_reads": 312,
  "oracle_avg_physical_reads": 8,
  "oracle_plan_hash": "3829471023",
  "oracle_plan_rows": [
    {"id": 0, "operation": "SELECT STATEMENT", "options": "", "object_name": ""},
    {"id": 1, "operation": "TABLE ACCESS", "options": "BY INDEX ROWID", "object_name": "ORDERS"},
    {"id": 2, "operation": "INDEX", "options": "RANGE SCAN", "object_name": "IDX_ORDERS_STATUS"}
  ]
}
```

### 3.3 stream 模式增量采集

```python
# 每 N 秒轮询 V$SQL 增量（按 LAST_ACTIVE_TIME 水位）
last_active_ts = "1970-01-01"
while True:
    new_rows = query("""
        SELECT SQL_ID, SQL_TEXT, EXECUTIONS, AVG_ELAPSED, LAST_ACTIVE_TIME, ...
        FROM V$SQL
        WHERE LAST_ACTIVE_TIME > :ts
          AND PARSING_SCHEMA_NAME IN :schemas
    """, ts=last_active_ts)
    append_to_workload(new_rows)
    last_active_ts = max(row.last_active_time for row in new_rows)
    sleep(interval)
```

---

## 4. 阶段二：回放层（sql_replay.py + sql_audit_daemon.py）

### 4.1 OB 诊断能力分级

| 级别 | 条件 | 可采集指标 |
|------|------|-----------|
| L0 基础 | obclient + EXPLAIN EXTENDED | 执行计划、wall time |
| L1 SQL Audit | `ob_enable_sql_audit=ON` | 完整时间拆分、缓存命中、RPC 标志 |
| L2 Plan Monitor | 同 L1，`GV$SQL_PLAN_MONITOR` | 算子级 output_rows、workarea_mem、下盘 |
| L3 PL Profiler | OB >= 4.2.3，`DBMS_PROFILER` | PL/SQL 行级耗时 |
| L4 OCP | OCP API 可用 | ASH 报告、租户级 QPM 趋势 |

工具启动时自动探测，能力写入 `replay_capability_<ts>.json`。

### 4.2 SQL Audit 守护进程（sql_audit_daemon.py）

**背景**：`GV$OB_SQL_AUDIT` 是环形缓冲区，硬上限 **1000 万行**，后台每 **500ms** 检查内存水位（90% 触发淘汰）。高并发回放时可能在数分钟内击穿上限导致关键数据丢失。

**实现**：与 `sql_replay.py` 并发运行的独立线程，轮询间隔 **300ms**（< 500ms 检查周期）：

```python
# sql_audit_daemon.py
last_request_id = 0
while replay_is_running:
    rows = ob_query("""
        SELECT REQUEST_ID, TRACE_ID, SQL_ID,
               ELAPSED_TIME, QUEUE_TIME, GET_PLAN_TIME, EXECUTE_TIME, NET_TIME, NET_WAIT_TIME,
               IS_HIT_PLAN, IS_EXECUTOR_RPC, TABLE_SCAN, RETRY_CNT,
               PLAN_TYPE, ROW_CACHE_HIT, BLOCK_CACHE_HIT,
               MEMSTORE_READ_ROW_COUNT, SSSTORE_READ_ROW_COUNT, BLOOM_FILTER_FILTERED_COUNT,
               LOGICAL_READ_COUNT, PHYSICAL_READ_COUNT
        FROM GV$OB_SQL_AUDIT
        WHERE TENANT_ID = :tid
          AND REQUEST_ID > :last_id
        ORDER BY REQUEST_ID
        LIMIT 5000
    """, tid=tenant_id, last_id=last_request_id)
    if rows:
        append_jsonl(audit_dump_file, rows)
        last_request_id = rows[-1].request_id
    sleep(0.3)
```

### 4.3 回放执行流程

```
for each sql in workload.jsonl:
    1. 绑定变量替换（bind_vars → prepared statement 或 literal）
    2. SET ob_query_timeout = oracle_avg_elapsed_us * timeout_factor * 1000
    3. 执行 SQL，记录 wall time（time.perf_counter）
    4. EXPLAIN EXTENDED → 解析算子树
    5. 从 audit_dump 按 TRACE_ID 关联该次执行的 SQL Audit 记录
    6. 若 plan_type = DISTRIBUTED 且 speedup_ratio < threshold：
       查 GV$SQL_PLAN_MONITOR（by EXECUTION_ID）→ 算子级监控
    7. 追加到 replay_<ts>.jsonl
```

### 4.4 replay JSONL schema

```json
{
  "sql_id": "8f2a1c3d9e4b0f17",
  "ob_status": "ok",
  "ob_error_code": null,
  "ob_wall_time_us": 12300,
  "ob_elapsed_us": 11800,
  "ob_queue_time_us": 180,
  "ob_get_plan_time_us": 320,
  "ob_execute_time_us": 11200,
  "ob_net_time_us": 8400,
  "ob_net_wait_time_us": 200,
  "ob_plan_type_raw": 3,
  "ob_plan_type": "DISTRIBUTED",
  "ob_is_hit_plan": false,
  "ob_is_executor_rpc": true,
  "ob_table_scan": false,
  "ob_retry_cnt": 0,
  "ob_logical_reads": 88,
  "ob_physical_reads": 3,
  "ob_row_cache_hit": 0,
  "ob_block_cache_hit": 12,
  "ob_memstore_read_rows": 45000,
  "ob_ssstore_read_rows": 2300,
  "ob_bloom_filter_filtered": 18000,
  "ob_plan_hash": "9912ab34",
  "ob_plan_rows": [...],
  "speedup_ratio": 0.26,
  "plan_changed": true,
  "read_amplification": 0.28,
  "ob_plan_monitor": null,
  "replayed_at": "2026-04-22T10:05:00"
}
```

**plan_type 映射**（源自 `src/sql/ob_sql_define.h`）：

| raw 值 | 名称 | 含义 |
|--------|------|------|
| 1 | `LOCAL` | 所有分区 Leader 在本节点，性能最优 |
| 2 | `REMOTE` | 需 RPC 访问远端节点 |
| 3 | `DISTRIBUTED` | 跨多节点并行执行，最复杂 |
| 4 | `UNCERTAIN` | 编译期无法确定（含参数化分区键）|

### 4.5 关键指标定义

| 指标 | 计算方式 | 信号含义 |
|------|---------|---------|
| `speedup_ratio` | `oracle_avg_elapsed / ob_elapsed` | < 1 = OB 慢 |
| `plan_changed` | `oracle_plan_hash != ob_plan_hash` | 执行计划变化 |
| `read_amplification` | `ob_logical_reads / oracle_avg_logical_reads` | 读放大倍数 |
| `net_ratio` | `ob_net_time / ob_elapsed` | 网络开销占比（分布式惩罚信号）|
| `plan_miss_ratio` | `!ob_is_hit_plan` | 硬解析频率 |
| `lsm_memstore_ratio` | `ob_memstore_read_rows / (ob_memstore_read_rows + ob_ssstore_read_rows)` | 增量数据占比（转储压力信号）|
| `replay_success_rate` | `ok_count / total_count` | 对齐 OMA 指标 |

---

## 5. 阶段三：分析报告层（perf_report.py）

### 5.1 报告结构

```
perf_report_<ts>.html
├── 0. 总览面板
│   ├── 回放成功率（ok / timeout / error / skip 分类）
│   ├── 全局 QPM 对比时序图（Oracle vs OB）
│   └── 性能分布饼图（OB 提速 / 持平 / 轻微回退 / 严重回退）
│
├── 1. 慢查询差异 Top-N（默认 Top 50，按 speedup_ratio 升序）
│   ├── 列：SQL摘要 / Oracle均值 / OB均值 / 加速比 /
│   │       逻辑读放大 / 计划类型 / 计划变化 / 触发规则
│   └── 每行可展开：Oracle 执行计划 ↔ OB 执行计划 并排对比
│
├── 2. 执行计划变化清单（plan_changed = True）
│   └── 算子翻译矩阵高亮差异节点
│
├── 3. 专家优化建议（规则引擎输出）
│
└── 4. 回放错误分类
    ├── 语法兼容性错误（ORA-xxxxx）
    ├── 超时（> oracle_avg × timeout_factor）
    └── 权限 / 对象缺失
```

### 5.2 算子翻译矩阵（plan_operator_map.py）

基于 `src/sql/engine/ob_phy_operator_type.h` 源码：

| Oracle 算子 | OB 算子 | 风险信号 |
|------------|---------|---------|
| `TABLE ACCESS BY INDEX ROWID` | `TABLE SCAN` | 正常（本地索引回表封装在 TABLE SCAN 内）|
| `TABLE ACCESS BY INDEX ROWID` | `TABLE LOOKUP` | 高风险：全局索引跨节点回表 RPC |
| `NESTED LOOPS` | `JOIN (NL)` | 若 plan_type=DISTRIBUTED → RPC 风暴 |
| `PX COORDINATOR` | `PX COORDINATOR` | 检查 DFO EXCHANGE IN/OUT 拥塞 |
| `PARTITION RANGE SINGLE` | `PARTITION SCAN` | 验证分区裁剪是否生效 |
| `HASH JOIN` | `HASH JOIN` | 检查 workarea_tempseg > 0（下盘）|
| `SORT` | `SORT` | 检查 workarea_tempseg > 0（下盘）|

### 5.3 专家规则引擎（optimization_rules.py）

规则以 Python dataclass 定义，追加新规则不修改核心逻辑。

#### 规则 DIST-JOIN：分布式执行计划惩罚

**触发**：`ob_plan_type = DISTRIBUTED` 且 `net_ratio > 0.6`

**诊断**：跨节点关联，网络开销占主导，JOIN 的两表分区 Leader 不在同一节点。

**建议**（自动生成 DDL）：
```sql
-- MPES 建议：将频繁 JOIN 的表绑定到同一 Table Group
CREATE TABLEGROUP tg_order_item;
ALTER TABLE orders SET TABLEGROUP tg_order_item;
ALTER TABLE order_items SET TABLEGROUP tg_order_item;
-- 确保两表分区策略、Locality、Primary Zone 完全一致
```

#### 规则 PX-SKEW：并行执行倾斜

**触发**：`GV$SQL_PLAN_MONITOR` 中某线程 `output_rows` > 均值 200% 或 `workarea_tempseg > 0`

**诊断**：DFO 数据分布不均，或 Hash Join/Sort 算子被迫下盘（Spill to Disk）。

**建议**：
```sql
-- 干预并行调度器数据分发
SELECT /*+ PQ_DISTRIBUTE(t1 HASH HASH) */ ...
-- 或检查哈希分区键是否存在数据倾斜
```

#### 规则 PLSQL-RPC：PL/SQL 游标循环 RPC 风暴

**触发**（OB >= 4.2.3）：`plsql_profiler_data` 定位到循环行 + 同时段 `IS_EXECUTOR_RPC` 线性增长

**诊断**：FOR 游标逐行 DML 在分布式环境下退化为每次迭代一次跨节点 RPC。

**建议**（附代码模板）：
```sql
-- 方案一：FORALL 批量操作（替换逐行游标）
FORALL i IN 1..v_ids.COUNT
  UPDATE orders SET status = 'DONE' WHERE id = v_ids(i);

-- 方案二：改写为单条 MERGE INTO（推荐）
MERGE INTO orders t
USING (SELECT id FROM batch_source WHERE ...) s
ON (t.id = s.id)
WHEN MATCHED THEN UPDATE SET t.status = 'DONE';
```

#### 规则 LOCK-HOT：热点行锁竞争

**触发**：`queue_time > execute_time * 3` 且 `retry_cnt > 0`

**诊断**：高并发 SELECT FOR UPDATE 导致工作线程池排队耗尽。

**建议**：
```sql
-- 追加 NOWAIT，令锁等待立即抛异常，应用层退避重试
SELECT * FROM accounts WHERE id = :1 FOR UPDATE NOWAIT;
```

#### 规则 PLAN-MISS：计划缓存穿透

**触发**：`is_hit_plan = false` 持续 且 `get_plan_time / elapsed > 0.2`

**诊断**：频繁硬解析，可能由统计信息缺失或 SQL 参数化失败引起。

**建议**：
```sql
-- 收集列直方图
ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS status, created_at;

-- 强制计划绑定（Plan Baseline）
-- 先开启优化器 Trace 获取最优计划
CALL DBMS_XPLAN.ENABLE_OPT_TRACE();
-- 执行目标 SQL 后获取最优 plan_hash
-- 然后创建 Outline 或 Binding
```

#### 规则 LSM-JITTER：LSM-Tree 存储层异常

**触发**：`lsm_memstore_ratio > 0.7` 持续上涨 且 `bloom_filter_filtered` 低

**诊断**：增量数据（MemTable）堆积，触发转储/合并操作导致响应时间抖动。

**建议**：
- 控制单批处理行数（ORDER BY + LIMIT 分页）
- 将 Major Compaction 错峰至业务低谷期
- 检查 `ob_compaction_schedule_interval` 参数

### 5.4 输出文件

| 文件 | 用途 |
|------|------|
| `perf_report_<ts>.html` | 主报告，浏览器直接打开 |
| `perf_report_<ts>_summary.txt` | 纯文本摘要，可 cat / 邮件发送 |
| `perf_hints_<ts>.sql` | 所有建议的 SQL/DDL 片段，可直接复制执行 |

---

## 6. 结果集验证层（后续迭代）

仅对 SELECT 语句生效，需显式开启 `--verify-results`。

```
策略：行级摘要对比，不存全量结果集

Oracle: fetchall() → 排序后 MD5
OB:     fetchall() → 排序后 MD5
                      ↓
            MD5 一致 → result_match = True
            MD5 不一致 → 采样前 20 行 diff → mismatch_<ts>.jsonl
```

**采样门限**：单 SQL 结果集 > 10,000 行时跳过（标注 `result_check=skipped/too_large`）。

---

## 7. 主入口 CLI（perf_comparator.py）

### 7.1 命令示例

```bash
# 批量模式
python3 perf_comparator.py --mode batch \
    --top-n 100 --min-exec 10 --hours 24

# 流式模式
python3 perf_comparator.py --mode stream \
    --interval 60 --duration 3600

# 仅回放
python3 perf_comparator.py --mode replay-only \
    --workload workloads/workload_20260422_100000.jsonl

# 仅报告
python3 perf_comparator.py --mode report-only \
    --replay workloads/replay_20260422_100500.jsonl

# 含结果集验证
python3 perf_comparator.py --mode batch \
    --verify-results --result-sample-limit 5000
```

### 7.2 关键参数

| 参数 | 默认值 | 说明 |
|------|-------|------|
| `--mode` | `batch` | 运行模式 |
| `--top-n` | 50 | 报告展示慢查询数量 |
| `--min-exec` | 5 | 过滤执行次数过少的 SQL |
| `--hours` | 24 | AWR/V$SQL 捕获时间窗口 |
| `--timeout-factor` | 3.0 | OB 超时门限 = oracle均值 × N |
| `--slowdown-threshold` | 0.8 | speedup_ratio 低于此值标红 |
| `--interval` | 60 | stream 模式轮询间隔（秒）|
| `--audit-poll-ms` | 300 | SQL Audit daemon 轮询间隔（毫秒）|
| `--verify-results` | False | 开启结果集正确性验证 |
| `--wcr-path` | None | Oracle WCR 文件路径 |
| `--sql-file` | None | 手动 SQL 文本文件路径 |

### 7.3 配置文件（config.ini.template）

```ini
[ORACLE_SOURCE]
user = 
password = 
dsn = 

[OCEANBASE_TARGET]
host = 
port = 2881
user = 
password = 
database = 
tenant_id = 1002

[SETTINGS]
source_schemas = SCHEMA1,SCHEMA2
ob_version = 4.2.5
ob_enable_sql_audit = true
```

---

## 8. OB 源码关键参照

以下字段来自 OceanBase 源码直接核查，确保采集精确性：

### GV$OB_SQL_AUDIT 核心字段（src/observer/virtual_table/ob_gv_sql_audit.h）

| 字段 | 列ID | 结构体字段 | 含义 |
|------|------|-----------|------|
| ELAPSED_TIME | 120 | `ObExecTimestamp.elapsed_t_` | 总耗时 (us) |
| QUEUE_TIME | 123 | `ObExecTimestamp.queue_t_` | 队列等待 |
| GET_PLAN_TIME | 125 | `ObExecTimestamp.get_plan_t_` | 获取计划耗时 |
| EXECUTE_TIME | 126 | `ObExecTimestamp.executor_t_` | 纯执行耗时 |
| NET_TIME | 121 | `ObExecTimestamp.net_t_` | 网络传输耗时 |
| NET_WAIT_TIME | 122 | `ObExecTimestamp.net_wait_t_` | 网络等待耗时 |
| IS_HIT_PLAN | 117 | `ObAuditRecordData.is_hit_plan_cache_` | 计划缓存命中 |
| IS_EXECUTOR_RPC | 116 | `ObAuditRecordData.is_executor_rpc_` | 跨节点 RPC |
| TABLE_SCAN | 138 | `ObAuditRecordData.table_scan_` | 全表扫描 |
| RETRY_CNT | 137 | `ObAuditRecordData.try_cnt_` | 重试次数（锁竞争）|
| PLAN_TYPE | 113 | `ObAuditRecordData.plan_type_` | 计划类型 1-4 |
| ROW_CACHE_HIT | 131 | `ObExecRecord.row_cache_hit_` | 行缓存命中 |
| BLOCK_CACHE_HIT | 133 | `ObExecRecord.block_cache_hit_` | 块缓存命中 |
| MEMSTORE_READ_ROW_COUNT | - | `ObExecRecord.memstore_read_row_count_` | MemTable 读（LSM 增量）|
| SSSTORE_READ_ROW_COUNT | - | `ObExecRecord.ssstore_read_row_count_` | SSTable 读（LSM 基线）|
| BLOOM_FILTER_FILTERED | - | `ObExecRecord.bloom_filter_filts_` | 布隆过滤器拦截 |

### 环形缓冲区淘汰参数（src/observer/mysql/ob_mysql_request_manager.h）

```cpp
MAX_QUEUE_SIZE = 10,000,000          // 1000 万行硬上限
HIGH_LEVEL_EVICT_PERCENTAGE = 0.90   // 内存达到 90% 触发淘汰
LOW_LEVEL_EVICT_PERCENTAGE  = 0.80   // 淘汰到 80% 停止
CONSTRUCT_EVICT_INTERVAL = 500,000us // 500ms 检查周期
```

→ **daemon 轮询间隔必须 < 500ms**，设计值取 300ms。

### GV$SQL_PLAN_MONITOR 关键字段（src/share/diagnosis/ob_sql_plan_monitor_node_list.h）

```cpp
output_row_count_    // 算子输出行数（并行倾斜检测）
rescan_times_        // 重扫次数
workarea_mem_        // 当前工作区内存
workarea_max_mem_    // 峰值工作区内存
workarea_tempseg_    // 临时磁盘（> 0 = 下盘）
workarea_max_tempseg_
```

### DBMS_PROFILER 接口（src/pl/sys_package/ob_dbms_profiler.cpp，OB >= 4.2.3）

```sql
-- 初始化（创建 profiler 系统表，首次使用）
CALL DBMS_PROFILER.OB_INIT_OBJECTS(FALSE);

-- 采集
CALL DBMS_PROFILER.START_PROFILER(:run_comment);
-- 执行目标 PL/SQL
CALL DBMS_PROFILER.STOP_PROFILER();

-- 查询行级耗时
SELECT d.line#, d.total_occur, d.total_time, d.min_time, d.max_time
FROM plsql_profiler_data d
WHERE d.runid = :runid
ORDER BY d.total_time DESC;
```

---

## 9. 设计决策记录

| 决策 | 选择 | 理由 |
|------|------|------|
| 架构模式 | 阶段解耦 + 中间 JSONL 文件 | 各阶段可独立调用，支持断点续跑，中间文件可移交分析 |
| 在线旁路 | 延迟旁路（Oracle Audit/V$SQL 追流）| 客户内网不允许修改应用和网络拓扑 |
| SQL Audit 保护 | 300ms 轮询守护进程 | 规避 1000 万行 / 500ms 检查周期的淘汰风险 |
| 规则引擎 | Python dataclass 可扩展规则 | 追加新规则不修改核心逻辑 |
| OMA 关系 | 概念对齐（QPM / 回放成功率 / 三模式命名）| 不直接依赖 OMA 二进制，保持独立可运行 |
| 结果集验证 | 后续迭代，MD5 摘要对比 | 优先交付性能差异报告，避免全量结果集内存爆炸 |

---

## 10. 迭代路线

| 阶段 | 内容 |
|------|------|
| **v1.0** | 捕获层（AWR/V$SQL）+ 回放层（L0/L1）+ 基础报告（Top-N + R01-R06 规则）|
| **v1.1** | GV$SQL_PLAN_MONITOR 算子级分析（PX-SKEW 规则）+ PL/SQL Profiler（PLSQL-RPC 规则）|
| **v1.2** | Stream 模式 + OCP 集成（L4）|
| **v2.0** | 结果集正确性验证 + mismatch 分析报告 |
