## Why

`perf_comparator` 现在已经能做：

- Oracle -> OceanBase 批量捕获、回放、报告
- Oracle `stream` 增量捕获
- OceanBase `source-report` 滚动诊断

但还有两处不满足生产长跑场景：

1. Oracle -> OB 的 `stream` 还是先抓完再统一回放和出报告，不适合 24 小时持续监控
2. OceanBase 源端如果使用非 `SYS` 用户，`GV$OB_SQL_AUDIT.QUERY_SQL` 往往不可见，当前提醒不够显著

客户需要：

- 后台持续运行
- 不遗漏新的 SQL 和 PL/SQL 调用类型
- 持续把新增工作负载回放到 OB
- 持续刷新同一组报告文件
- 运行一开始就明确知道 `QUERY_SQL` 可见性风险和处理方式

## What Changes

- 增强 `stream` 模式，使其在 Oracle -> OB 链路上支持滚动回放和滚动报告刷新
- 将 Oracle 采集上限与报告展示上限解耦，避免长跑采集只抓到少量热点
- 在 OceanBase 源端使用非 `SYS` 登录时，于启动期输出显著告警
- 在 source-only 报告中增加更醒目的 `QUERY_SQL` 可见性提示
- 在 replay 报告中增加慢 SQL / 慢 PL/SQL 分区，便于长期窗口排查

## Impact

- 影响 CLI 编排、Oracle stream 捕获、报告生成、配置模板与运行文档
- 不引入新服务，不改变单文件 Python 运行约束
