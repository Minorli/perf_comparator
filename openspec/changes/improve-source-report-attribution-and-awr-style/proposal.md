## Why

实库验证暴露了两个问题：

1. `source-report` 的 caller group 归因会被无 attribution 的 plan-cache / sqlstat 补充行污染，导致 top caller group 偏向 `schema=...|sql_id=...` 这种退化键
2. 当前 HTML/TXT 报告虽然信息够用，但展示层次还不够高级，缺少 Oracle AWR 那种：
   - 明确的导航结构
   - SQL ID / SQL 文本的快速跳转
   - 高价值 Top 区块和明细区块联动

## What Changes

- 修正 source-only caller attribution，优先使用 audit-backed attribution，避免 fallback actor 覆盖真实 caller group
- 增加 attribution quality / coverage 统计，让用户知道哪些热点是“真 caller”、哪些是 fallback
- 从 Oracle 实库生成一份真实 AWR HTML 报告，提炼其版式和高价值内容
- 将 AWR 风格借鉴到 OceanBase source-only 和 Oracle->OB replay HTML 报告：
  - 顶部目录导航
  - Top SQL / Top PL/SQL 摘要跳转
  - SQL ID anchor
  - SQL 文本详情折叠区
  - 更清晰的 evidence / cause / actor 展示

## Impact

- 影响 `source-report` 聚合逻辑
- 影响 source-only 和 replay 两类 HTML 报告
- 增加一份 AWR 参考产物和文档说明
