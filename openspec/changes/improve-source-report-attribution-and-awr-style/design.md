## Context

当前 `source-report` 会把三类来源合并：

- `GV$OB_SQL_AUDIT`
- `GV$OB_PLAN_CACHE_PLAN_STAT`
- `GV$OB_SQLSTAT`

其中只有 audit 行稳定带 caller attribution。补充来源虽然对 SQL 文本和长期热点很有价值，但经常没有 `tenant/db/user/client_ip`，导致聚合时 actor fallback 占了上风。

与此同时，报告展示层次仍偏“工程输出”，还没达到 AWR HTML 的“运维/客户一眼可读”级别。

## Goals

- top caller groups 必须优先反映真实 audit caller
- 报告明确标记 attribution quality
- HTML 报告借鉴 AWR 的结构化导航和 SQL drill-down 体验

## Non-Goals

- 不复制 Oracle AWR 的全部区块
- 不实现前端 SPA 或独立 Web 服务

## Decisions

### 1. caller group 只用 attribution-backed samples 排序

对 source-only 聚合增加两套计数：

- `source_actor_counts`: 仅统计真实 attribution 行
- `source_fallback_actor_counts`: 统计无 attribution 的 fallback 行

主排序、top caller group、primary actor 全部基于前者。
只有当某个 SQL 完全没有 attribution-backed 行时，才退回 fallback actor。

### 2. 报告显式展示 attribution quality

每条热点增加：

- `attribution=direct|fallback|mixed`
- `direct_samples`
- `fallback_samples`

这样用户能判断 caller 归因是否可靠。

### 3. 借鉴 AWR 的“目录 + Top 区块 + 明细 anchor”结构

从真实 AWR HTML 中借鉴这些展示原则：

- 顶部目录导航
- 关键 Top 区块放前
- 每条 SQL 通过 SQL ID 可跳到明细
- SQL 文本单独展开，避免主表太宽
- 章节标题稳定，可用于客户沟通和截图

### 4. source-only 与 replay 报告统一为“摘要导航 + 明细卡片”

两类报告统一增加：

- 目录区
- Top Slow SQL
- Top Slow PL/SQL
- Top Caller Groups（仅 source-only）
- Detailed Findings

Detailed Findings 中每条热点拥有：

- anchor: `sql-<sql_id>`
- SQL ID、actor、type、cause、timing、rules、monitor、plan risk
- SQL 文本折叠区

## Risks

- HTML 报告体积会增大
  - 通过折叠 SQL 文本控制
- attribution 规则更严格后，部分 caller 可能变成 `unattributed`
  - 通过 `attribution=fallback` 明确标识，而不是继续误导
