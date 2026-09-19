# SparkAdvisor 规则设计（rules.md）

| 项 | 值 |
|---|---|
| 文档 | 《SparkAdvisor 规则设计》——规则的**单一事实源** |
| 版本 | v1.1，增加 S-30 LIMIT 初始分区建议 |
| 配套 | 《SparkAdvisor 设计文档》（架构、解析层、指标仓库、报告、CLI、工程结构） |
| 适用 | Spark 3.5.6 长驻共享查询队列（单 Application，1 Driver + 固定 Executor，每日 01:52 重启，一轮约 22h、数百至上千条 SQL） |
| 规则总数 | 50 条：S 系列（单 SQL/Stage 级）30 条、Q 系列（队列级）18 条、DQ 系列（数据质量）2 条 |

---

## 1. 定位与通用约定

### 1.0 本文档的角色

本文档定义每一条规则的**触发公式、默认阈值、证据字段、建议与生效层级**，是运行时规则行为的唯一权威来源。设计文档 §5 仅保留规则引擎框架（`Rule` 接口、`Finding` 契约、severity/score 排序、partial 降级）与一张总览索引表；当两份文档在阈值或语义上出现分歧时，**以本文档为准**，并在 CI 中对二者做一致性校验。

规则引擎的运行前提（来自设计文档，不在此展开）：规则是**纯函数** `evaluate(MetricsContext, Thresholds) → List<Finding>`，只消费指标仓库产出的聚合数据，从不读取原始 event log、不做数值提取；数值计算全部在解析层完成。因此本文档所有"触发公式"引用的字段，均指指标仓库 schema（设计文档 §4.3）中的已聚合字段。

### 1.1 规则 ID 稳定性

- 规则 ID（`S-01`/`Q-13`/`DQ-01`）一经发布**永不复用、永不重排**。历史 findings.jsonl、报告锚点、suppressions 配置、LLM 层的引用都依赖 ID 稳定。
- 废弃某规则时，保留其 ID 并标注 `DEPRECATED`，不把编号让给新规则。
- 因此分组内编号允许不连续（如 C 组含 S-11/S-12/S-23/S-24）——这是有意的，不是笔误。
- 新规则一律取"当前系列最大编号 +1"，与它落在哪个主题分组无关。

### 1.2 建议生效层级（action type）

每条 finding 的建议按**生效方式**归入四层，报告与 findings 均按此着色分组。四层的关键差异在于"多久生效、影响面多大"：

| 层级 | 生效时机 | 影响面 | 典型动作 |
|---|---|---|---|
| `SESSION_SET` | 当次会话 `SET` 即时生效 | 单会话/单 SQL | AQE skewJoin 参数、advisory 分区尺寸、autoBroadcastJoinThreshold |
| `RESTART_CONF` | 依赖 executor/driver 进程参数，**次日 01:52 重启窗口生效** | 整个队列 | executor 内存/核数、driver 资源、locality.wait、scheduler.mode、listenerbus capacity |
| `REWRITE` | 改 SQL 后下次执行生效 | 单指纹 | join hint、加盐、补分区谓词、REBALANCE/REPARTITION、limit/落表 |
| `GOVERNANCE` | 数据/表级治理，跨多条 SQL | 全局 | 源表/目标表 compaction、分区设计、统计信息收集 |

**每日 01:52 重启构成天然 A/B 边界**：`RESTART_CONF` 类变更在重启窗口一次性生效，前后两轮即可对照评估效果，这是本工具建议体系区别于通用调优清单的核心利用点。报告会显式标注每条 `RESTART_CONF` 建议"将在下次重启生效"。

### 1.3 记号

- `q.<字段>.<分位>`：stage 聚合分位数，如 `q.task_duration.p50`（源自 stages.jsonl 的 `q` 块）。
- `T_xxx(默认值)`：外置阈值符号与其默认值，如 `T_skew_ratio(5)`。所有阈值集中于 §7 的 conf.yaml，规则代码只读符号名。
- `queue_ctx`：SQL/STAGE 级 finding 自动注入的队列上下文切片（`busy_ratio_avg`、`busy_ratio_p95`、`concurrent_statements_avg`），源自 occupancy 时间线，让每条单点结论天然携带"当时队列什么状态"。
- WARN/CRITICAL：绝大多数规则给双档阈值；**启发式规则**（依赖计划文本字符串匹配等脆弱信号）封顶 WARN，不给 CRITICAL。

### 1.4 规则静默机制（suppressions）

生产中总有"已知且不打算改"的命中（如某历史遗留表的小文件、某条已排期改写的 SQL）。为避免这类噪声长期占据报告头部，引擎支持按 **规则 × 指纹/语句** 粒度静默：

```yaml
suppressions:
  - rule: S-05
    fingerprint: "sha256:ab12..."      # 仅静默该指纹在该规则上的命中
    reason: "legacy_dim_table 已排期 2026Q3 compaction"
    until: "2026-09-30"                 # 可选，到期自动恢复
  - rule: S-19
    table: "ods.huge_log"              # 按表静默（S-05/06/19/Q-10 支持 table 维度）
    reason: "该表确认无分区，启发式误报"
  - rule: Q-04
    statement_id: "d3f1..."            # 按 statement 静默垄断告警（已知大 ETL）
    reason: "夜间大 ETL，业务已知并接受"
```

静默的 finding 仍会**生成但标记 `suppressed=true`**，不参与报告头部排序与"今天最值得做的三件事"，但保留在 findings.jsonl 中供审计与 LLM 层参考（避免"明明命中却查不到"）。静默是规则×对象粒度，不是全局关规则——同一规则对其他对象照常告警。

---

## 2. 规则总览

### 2.1 分组索引

规则按主题分组便于阅读，但**分组不影响 ID 编号**（§1.1）。★ 标记核心规则；⚑ 标记 M2 首批实现的 12 条。

| 组 | 单 SQL / Stage 级 (S) | 队列级 (Q) |
|---|---|---|
| A 数据分布与并行度 | S-01 耗时倾斜⚑ · S-02 字节倾斜 · S-03 分区过多⚑ · S-04 分区过少 · S-05 扫描小文件⚑ · S-06 输出小文件⚑ | Q-10 小文件全局治理清单 |
| B 内存与 CPU | S-07 Spill⚑ · S-08 GC 高 · S-09 内存峰值余量 · S-10 CPU 画像 | Q-07 全天资源健康趋势 · Q-14 内存不足综合诊断 |
| C Shuffle / 网络 / 磁盘 | S-11 Fetch Wait 高 · S-12 序列化/ResultSize 异常 · S-23 Shuffle 写吞吐异常 · S-24 数据本地性劣化 | Q-12 节点 IO 吞吐异常 · Q-13 Shuffle 网络异常矩阵 |
| D 调度与关键路径 | S-13 Scheduler Delay 高 · S-14 排队归因★⚑ · S-15 Driver 间隙 · S-16 关键路径分解⚑ · S-27 Job 碎片化 | Q-01 占用时间线★⚑ · Q-02 容量利用率⚑ · Q-03 排队热点 · Q-04 单语句垄断 · Q-11 系统性慢时段 · Q-15 Driver 瓶颈画像 |
| E 执行计划 | S-17 广播机会 · S-18 广播风险 · S-19 分区裁剪疑似失效 · S-20 AQE 干预提示 · S-25 危险 Join 算子 · S-26 Join/Explode 行数放大 · S-29 Codegen 失效热点 | — |
| F 稳定性 | S-21 Task 失败画像⚑ · S-28 推测执行效果评估 | Q-05 慢节点 · Q-06 Executor 异常 · Q-16 失败/重试风暴 · Q-17 重启窗口影响评估 |
| G 基线 | S-22 指纹回归与计划漂移⚑ | Q-08 静态配置体检 · Q-09 吞吐与 TopN 榜单⚑ · Q-18 队列级基线漂移 |
| H 数据质量 | — | DQ-01 事件完整性 · DQ-02 时间一致性 |

M2 首批 12 条（⚑）：S-01/03/05/06/07/14/16/21/22 + Q-01/02/09——覆盖"最高频、最高价值、最易验证"的诊断，与人工看 UI 的定位结论可直接抽样比对。

### 2.2 生产故障域覆盖对照表

按"运维实际会遇到的故障域"反查规则，确认无盲区。一个故障域通常由多条规则从不同角度共同覆盖（单 SQL 视角 + 队列视角 + 基线视角）。

| 生产故障域 | 主要现象 | 覆盖规则 |
|---|---|---|
| CPU 打满 / 热点 | 核占满、CPU-bound stage | S-10 · Q-11 |
| 内存不足（Executor） | OOM、spill、GC 高、堆逼近上限 | S-07 · S-08 · S-09 · S-21(OOM 分支) · **Q-14**(综合诊断) · Q-06 |
| 内存不足（Driver） | broadcast OOM、大结果回传、driver 堆高 | S-12 · S-18 · **Q-15** |
| 磁盘 IO / 容量 | spill 落盘、shuffle 写慢、本地盘吞吐瓶颈 | S-07 · **S-23**(写吞吐) · **Q-12**(节点 IO 热点) |
| HDFS / 对象存储 | 小文件扫描、写放大、文件列举慢 | S-05 · S-06 · S-15(列举) · S-19(裁剪) · Q-10 |
| 网络 | shuffle fetch 慢、跨机架、拥塞 | S-11 · **S-24**(本地性) · **Q-13**(src×dst 矩阵) |
| 调度 | 排队、locality 等待、Driver 事件积压、Job 碎片化 | S-13 · S-14★ · **S-27**(碎片化) · Q-01★ · Q-02 · Q-03 · Q-04 |
| Driver 单点 (SPOF) | Driver GC、规划间隙、事件队列溢出 | S-15 · **Q-15** · S-13(积压分支) |
| 硬件 / 坏节点 | 单机慢盘慢网、局部离群 | Q-05 · **Q-12** |
| 进程存活 | Executor 丢失、频繁重启 | Q-06 · **Q-16**(风暴) |
| 认证 / 凭据 | Kerberos/delegation token 过期（长驻应用特有） | S-21(AUTH 分支) · **Q-16**(AUTH 分支) |
| 重启窗口 | 01:52 打断在途 SQL、轮末高风险提交 | **Q-17** |
| 回归 / 漂移 | 计划漂移、数据涨、个体回归 vs 全队列普涨 | S-22 · **Q-18** |
| 数据分布 / 倾斜 | task 耗时/字节离群、分区数不当 | S-01 · S-02 · S-03 · S-04 |
| 执行计划质量 | 漏广播、危险 join、行数放大、codegen 退化、裁剪失效 | S-17 · S-18 · S-19 · S-20 · **S-25** · **S-26** · **S-29** |
| 观测自身可信度 | 丢事件、时钟偏移 | **DQ-01** · **DQ-02** |

粗体为本轮为强化生产覆盖新增的 16 条（S-23~S-29 中的 7 条、Q-12~Q-18、DQ-01/02）。

### 2.3 依赖能力矩阵

部分规则依赖可选采集能力；能力缺失时引擎**自动跳过该规则**并在报告解析质量章节标注"因缺少 X 未评估"，绝不给出基于缺失数据的结论。`Rule.requires()` 声明依赖，引擎在评估前检查。

| 能力（Capability） | 来源 | 缺失后果 | 依赖规则 |
|---|---|---|---|
| `STAGE_EXECUTOR_METRICS` | `spark.eventLog.logStageExecutorMetrics=true`（SparkListenerStageExecutorMetrics 事件） | 无 executor/driver 内存峰值，内存类结论降级或跳过 | S-09 · Q-07 · Q-14 · Q-15 |
| `PLAN_METRICS` | sparkPlanInfo 的 accumulator → 计划节点指标映射（设计文档 §3.4） | 无文件数/字节/行数等计划节点指标 | S-05 · S-06 · S-17 · S-18 · S-25 · S-26 |
| `PLAN_TEXT` | physicalPlanDescription 原文 | 无法做计划文本字符串匹配 | S-19 · S-29 |
| `LIMIT_PROBE_METRICS` | 已完成 CollectLimit 的同 RDD 串行探测 Job、逐 Job 配置及核数快照 | 无法可靠识别取数轮次；核数未知时仅提示复核 | S-30 |
| `BASELINE` | baseline/fingerprint_rollup.jsonl（≥ N 轮历史） | 无历史对照，回归/漂移类不评估（首日必然缺失） | S-22 · Q-18 |
| `STATEMENT_ID`（关联链） | JobStart 的 jobGroup/自定义 property（设计文档 §3.3，需 validate 首日确认） | 关联率低时单 SQL 维度结论受限，队列维度仍可用 | 全部 S 系列的单 SQL 归属 |
| 基础 TaskMetrics | TaskEnd（始终可用） | —— | 其余全部规则 |

首日务必先跑 `sparkadvisor validate` 确认 `STATEMENT_ID` 提取键与各能力开关状态；建议在 Spark 端开启 rolling + `logStageExecutorMetrics`（设计文档 §3.9），以解锁 S-09/Q-07/Q-14/Q-15 这批高价值内存诊断。

---
## 3. S 系列：单 SQL / Stage 级规则

评估对象为单个 execution 或其下 stage。所有 S 规则在求值时自动注入 `queue_ctx`（§1.3），使"这条 SQL 慢"的结论天然能区分"自身问题"与"被队列拖累"（S-14 是这一区分的专职规则）。partial stage（`metrics_partial=true`）上，分位数类规则输出封顶 WARN 并标注置信度。

### 3.A 数据分布与并行度

**S-01 Task 耗时倾斜** ⚑
- 触发：`num_tasks ≥ T_min_tasks(20)` 且 `q.task_duration.max ≥ T_skew_abs(120s)` 且 `max / max(p50, 1s) ≥ T_skew_ratio(5)`。
- 证据：p50/p95/max、skew_ratio、Top-K task 全明细（ID/host/executor/bytes/GC，含 stderr URL）、AQE 是否已介入（S-20 联动）、queue_ctx（排除"慢 task 其实在等核"的假象：若该 task 的 scheduler_delay 占比高则移交 S-14）。
- 建议：join 侧倾斜 → SESSION_SET AQE skewJoin 三参数（`skewedPartitionFactor`/`skewedPartitionThresholdInBytes`/`advisoryPartitionSizeInBytes`）；聚合侧 → REWRITE 两阶段聚合/加盐；单 key 极端 → REWRITE 热点 key 前置分流。
- 层级：SESSION / REWRITE。

**S-02 Shuffle 字节倾斜**
- 触发：`q.shuffle_read_bytes.max / max(p50, 1MB) ≥ T_bytes_skew(8)` 且 `max ≥ T_bytes_abs(1GB)`。
- 与 S-01 互为佐证：字节倾斜但耗时不倾斜 → 提示"倾斜存在但被大内存掩盖，数据量增长后将恶化"（INFO 级前瞻预警，共享队列的隐患排查价值高）。
- 证据/建议：同 S-01，另给 top task 的 `shuffle_read_bytes` 与所在 reduce 分区 index。

**S-03 分区过多 / 碎片化 Task** ⚑
- 触发：`num_tasks ≥ T_many(2000)` 且 `q.task_duration.p50 ≤ T_tiny(2s)` 且 `(sched_delay.sum + deser.sum) / run_time.sum ≥ T_overhead(0.3)`。
- 证据：p50 时长、单 task 平均 shuffle read（通常 ≪ advisory 值）、调度开销占比。
- 建议：AQE 开启时 SESSION_SET `spark.sql.adaptive.coalescePartitions.enabled=true` 并核对 `advisoryPartitionSizeInBytes`（现值取自 conf.json，一并展示）；AQE coalesce 已开仍碎片 → 检查 `minPartitionSize`/上游分区数硬指定（REWRITE 去掉多余 repartition）。
- 备注：AQE 时代不再"预言"静态 `spark.sql.shuffle.partitions`，规则输出以 advisory 尺寸为核心。

**S-04 分区过少 / 巨型 Task**
- 触发：`q.shuffle_read_bytes.p50 ≥ T_huge(512MB)`，或（`q.task_duration.p50 ≥ 10min` 且 `num_tasks < alive_cores`，即并行度吃不满固定核数）。
- 证据：p50 bytes、num_tasks vs alive_cores、若伴随 spill 则联动 S-07。
- 建议：SESSION_SET 调小 advisory 尺寸 / 提高 `initialPartitionNum`；REWRITE 显式 `REPARTITION`。

**S-05 扫描侧小文件** ⚑（依赖 PLAN_METRICS）
- 触发：最终计划 scan 节点 `number of files read ≥ T_files(1000)` 且 `size of files read / files ≤ T_avg_file(8MB)`。
- 证据：文件数、平均文件大小、对应表名（从计划文本提取）、scan stage 的 task 数与 p50 时长。
- 建议：GOVERNANCE 对源表做 compaction（CarbonData 场景给出 compaction 命令提示，经 PlanAdapter SPI 扩展，设计文档 §9.4）；SESSION_SET 核对 `spark.sql.files.maxPartitionBytes` / `openCostInBytes` 使多小文件合并进单 split。
- 汇入 Q-10 全局清单。

**S-06 输出侧小文件** ⚑（依赖 PLAN_METRICS）
- 触发：写出类节点（InsertIntoHadoopFsRelation / CarbonData load，经 SPI 适配）`number of written files ≥ T_out_files(500)` 且 `written output / files ≤ T_out_avg(16MB)`。
- 证据：文件数、平均大小、目标表、是否动态分区写。
- 建议：REWRITE 写前 `/*+ REBALANCE */`（3.2+）或 `REPARTITION(n)`；动态分区写给出按分区键 repartition 的写法；GOVERNANCE 目标表定期 compaction。
- 汇入 Q-10 全局清单。

### 3.B 内存与 CPU

**S-07 Spill** ⚑
- 触发：`q.spill_disk_bytes.sum ≥ T_spill_abs(10GB)` 或 `spill_disk.sum / max(shuffle_write.sum + input.sum, 1) ≥ T_spill_ratio(0.2)`。
- 证据：mem/disk spill 总量、p95/max 单 task spill、伴随 GC 比、`peak_exec_mem` 分布、当前 executor 内存与 `spark.memory.fraction`（conf 快照）。
- 建议：优先 SESSION_SET 提高该查询分区数（摊薄单 task 数据）；结构性不足 → RESTART_CONF 提高 `spark.executor.memory` 或降低 `spark.executor.cores`（提高每 task 内存份额；固定 executor 场景需给出总吞吐权衡说明）；REWRITE 检查 explode/高基数聚合。
- 交叉引用：spill 是"内存不足"的最常见表征，队列级综合视角见 **Q-14**（区分单 SQL 黑洞 / 水位不足 / 并发叠加三种成因）。

**S-08 GC 占比过高**
- 触发：stage 级 `gc.sum / run_time.sum ≥ T_gc_warn(0.10)`（WARN）/ `≥ T_gc_crit(0.20)`（CRITICAL），且 `run_time.sum ≥ T_gc_min_runtime(10min)`（过滤噪声）。
- 证据：GC 占比、按 executor 的 GC 分布（个别 executor 高 → 联动 Q-05）、峰值内存。
- 建议：RESTART_CONF 内存/核配比、G1 参数；SESSION 侧先用 S-07 的分区手段降压。

**S-09 内存峰值余量**（依赖 STAGE_EXECUTOR_METRICS）
- 触发：某 executor `peak(JVMHeapMemory) ≥ T_mem_risk(0.9) × Xmx`（风险）或全天 `max(peak) ≤ T_mem_waste(0.5) × Xmx`（浪费）。
- 证据：峰值分布、发生 stage、driver 条目单列（driver 峰值高 → 联动 S-12/Q-15 收集类风险）。
- 建议：RESTART_CONF 双向 sizing（固定 executor 数不变的前提下，内存上调/下调的量化建议 = 峰值 × 1.25 取整）。

**S-10 CPU 画像**（INFO，定性）
- 触发：恒输出。`cpu.sum / run_time.sum ≥ T_cpu_bound(0.7)` → CPU-bound；`≤ T_io_bound(0.3)` 且 fetch_wait 低、GC 低 → IO-bound（扫描/写出为主）。
- 用途：为其他规则的建议定向（CPU-bound 时"加分区"收益有限；IO-bound 时压缩/文件布局收益大），并进入指纹画像。

### 3.C Shuffle / 网络 / 磁盘

**S-11 Fetch Wait 占比高**
- 触发：`fetch_wait.sum / run_time.sum ≥ T_fetch(0.15)` 且 shuffle_read ≥ T_fetch_min_bytes(1GB)。
- 证据：fetch_wait 占比、remote/local bytes 比、上游 map stage 的输出分布（上游倾斜会放大单点拉取）、按 host 的 fetch_wait 分布（集中单 host → 联动 Q-05 坏节点）。
- 建议：上游倾斜 → 走 S-01 路径；普遍偏高 → RESTART_CONF 核对 external shuffle service 压力、`spark.reducer.maxSizeInFlight`/`maxReqsInFlight`。
- 交叉引用：当 fetch 慢呈现"源/目的成对聚集"特征时，队列级判定单机/机架/拥塞见 **Q-13**（src×dst FetchFailed 矩阵）；本规则是单 SQL 视角的入口信号。

**S-12 序列化 / ResultSize 异常**
- 触发三分支：a) `deser.p95 ≥ T_deser(5s)`（大闭包/大广播反序列化）；b) `result_ser.sum / run_time.sum ≥ T_result_ser_ratio(0.1)`；c) `result_size.sum ≥ T_result(1GB)` 或单 task `result_size.max ≥ T_result_task(256MB)`。
- 证据：对应分位数、driver 峰值内存（S-09 联动）。
- 建议：a → REWRITE 检查 UDF 捕获的大对象/改广播变量；c → REWRITE 该 SQL 疑似大结果集回传（网关取数），建议 limit / 落表导出路径，并提示 `spark.driver.maxResultSize` 当前值与风险。
- 交叉引用：c 分支同时是 Driver 单点压力信号，队列级 Driver 画像见 **Q-15**。

**S-23 Shuffle 写吞吐异常**（新）
- 动机：spill（S-07）看的是"内存放不下"，本规则看的是"写盘本身慢"——直接的**本地磁盘**信号，共享队列里坏盘/满盘会以此先暴露。
- 触发：stage 有可观 shuffle 写（`shuffle_write.sum ≥ T_sw_min(10GB)`）且写吞吐 `shuffle_write_bytes.sum / shuffle_write_ms.sum ≤ T_sw_throughput(50MB/s)`（远低于本地盘正常水平）；或单 task `shuffle_write_ms` 占其 run_time 比 `≥ T_sw_ratio(0.3)`。
- 证据：写吞吐（MB/s）、按 host 的写吞吐分布（定位到具体节点）、伴随的磁盘 spill、该 host 是否同时命中 Q-05/Q-12。
- 建议：单 host 显著低 → 疑似坏盘/满盘，平台侧排查（报告明确这是 event log 内证据边界）并短期 RESTART_CONF exclude 该节点；普遍低 → 检查本地盘介质与 `spark.local.dir` 布局。
- 层级：RESTART_CONF / 平台侧。交叉引用：节点级 IO 热点聚合见 **Q-12**。

**S-24 数据本地性劣化**(新)
- 动机：固定 executor 拓扑下，本地性本应稳定；大面积 `ANY`/`RACK_LOCAL` 说明调度未能就近，放大网络与 fetch 压力。
- 触发：stage 的 locality 分布中 `(ANY + RACK_LOCAL) / total ≥ T_loc_bad(0.5)` 且 `num_tasks ≥ T_loc_min_tasks(200)`，排除纯 shuffle read stage（其 locality 语义不同）。
- 证据：locality 分布直方图、该 stage 的 fetch_wait 占比（与 S-11 互证）、`spark.locality.wait` 现值、是否伴随 executor 增删（拓扑抖动）。
- 建议：RESTART_CONF 调整 `spark.locality.wait`（共享队列常见收益：适度降低以减少等待，或结合数据放置评估）；若因 executor 分布不均，联动 Q-06。
- 层级：RESTART_CONF。

---
### 3.D 调度与关键路径

**S-13 Scheduler Delay 占比高**
- 触发：`sched_delay.p50 ≥ T_sd(1s)` 或 `sched_delay.sum / (stage 墙钟 × 并发)` 异常。
- 证据：与 S-03 区分——task 不小但 delay 高，指向 Driver 调度压力（事件积压、单 Driver 服务全队列）或 locality 等待（locality 分布佐证，与 S-24 互证）。
- 建议：RESTART_CONF `spark.locality.wait` 调低（固定 executor 的共享队列通常建议 0–1s）；Driver 压力 → RESTART_CONF driver 核数/内存、`spark.scheduler.listenerbus.eventqueue.capacity`（同时缓解丢事件，DQ-01）。
- 交叉引用：Driver 侧综合画像见 **Q-15**。

**S-14 排队等待归因（QUEUE vs SELF）** ★⚑
- 目的：直接回答"这条 SQL 慢，是排队还是自身瓶颈"——共享队列最高频的第一问。
- 算法：`wait_ratio = queue_wait_ms / duration_ms`；取该 execution 时间窗的 `queue_ctx.busy_ratio_avg`。三态判定矩阵：

| wait_ratio | busy_ratio | 结论 |
|---|---|---|
| ≥ T_wait(0.4) | ≥ T_busy(0.85) | **SLOW_DUE_TO_QUEUE**：慢主因是排队，调 SQL 无用，指向 Q-02/Q-03 |
| ≥ 0.4 | < 0.85 | **SCHEDULING_ANOMALY**：核有空却排队 → 检查 FAIR pool 配置 / 单语句 maxConcurrent 限制 / locality 等待，罕见但高价值 |
| < 0.4 | 任意 | **SELF**：进入 S-01…S-13、S-25…S-29 自身瓶颈分析 |

- 证据：queue_wait 分解（首 job 等待 + job 间隙）、时间窗内 occupancy 曲线切片、当时 Top-5 资源占用者（occupancy.top_consumers → 点名"谁挤占了我"）。
- 建议：QUEUE → 引用 Q-03 的时段建议（错峰/扩容）；ANOMALY → RESTART_CONF 调度器配置（如 FAIR pool）。

**S-15 Driver 间隙**
- 触发：`driver_gap_ms / duration_ms ≥ T_gap(0.3)` 且 `duration_ms ≥ T_gap_min(60s)`。
- 证据：间隙时间轴（各 job 之间的空洞）、间隙期间是否有"listing leaf files"类 job（文件列举以独立 job 出现时可识别）、AQE 重规划次数。
- 建议：大量分区列举 → GOVERNANCE 分区数治理 / SESSION_SET 相关 metastore 并行度参数；纯规划耗时 → 报告标注（复杂视图展开），供 REWRITE 评估。
- 交叉引用：Driver 单点画像见 **Q-15**。

**S-16 关键路径分解与长尾 Stage** ⚑
- 算法：以 execution 内 stage 的 `parent_ids` 建 DAG，跨 job 按时间顺序衔接；自末端回溯最长耗时链得关键路径；`coverage = Σ关键路径 stage 墙钟 / execution 墙钟`。
- 输出（恒输出，INFO；供报告"耗时都去哪了"章节）：关键路径 stage 列表及各自占比、最长杆 stage（其内部再按 p50×tasks / 长尾拆解）、非关键路径上的可并行机会。
- 价值：把"SQL 慢"翻译成"慢在 stage 15 的 shuffle read（占 61%）"，后续规则的建议全部锚定到关键路径 stage 上排序——**关键路径上的 S-17/S-25/S-29 命中，严重度加权上调**（§6）。

**S-27 Job 碎片化**（新）
- 动机：单条 SQL 被切成过多小 job（常见于逐分区/逐文件驱动、循环式 action、宽视图展开），Driver 侧调度与提交开销累积，且放大 S-15 间隙。
- 触发：`num_jobs ≥ T_frag_jobs(50)` 且 job 级 `median(job 墙钟) ≤ T_frag_job_ms(3s)` 且 `driver_gap_ms / duration_ms ≥ T_frag_gap(0.2)`。
- 证据：job 数、job 墙钟分布、driver_gap 占比、是否伴随大量小 stage（联动 S-03）。
- 建议：REWRITE 检查是否可批量化（合并循环 action / 避免逐分区触发）；GOVERNANCE 分区设计；平台侧检查网关是否逐语句提交。
- 层级：REWRITE / GOVERNANCE。

### 3.E 执行计划

**S-17 广播机会**（依赖 PLAN_METRICS）
- 触发：最终计划含 SortMergeJoin/ShuffledHashJoin，且其一侧 Exchange 的 `data size ≤ T_bc(64MB)`（或该侧 map stage `shuffle_write.sum` 小于阈值）。
- 证据：小侧实际字节/行数、当前 `spark.sql.autoBroadcastJoinThreshold`（conf 快照）、该 join 所在 stage 的耗时占比（关键路径联动，不在关键路径上则降 INFO）。
- 建议：REWRITE `/*+ BROADCAST(t) */`；或 SESSION_SET 提高 autoBroadcastJoinThreshold（给出安全上界=小侧字节×1.5，并提示统计缺失风险）。

**S-18 广播风险**（依赖 PLAN_METRICS）
- 触发：BroadcastExchange `data size ≥ T_bc_big(512MB)`，或 broadcast 相关超时/OOM 错误（error_class 联动），或 driver 峰值内存与广播时点吻合。
- 建议：SESSION_SET 降阈值 / REWRITE 去掉 broadcast hint；提示 `spark.sql.broadcastTimeout` 当前值。
- 交叉引用：Driver OOM 综合见 Q-15。

**S-19 分区裁剪疑似失效**（启发式，WARN 上限；依赖 PLAN_TEXT）
- 触发：计划文本中分区表 scan 的 `PartitionFilters: []` 且 `number of files read ≥ T_files_prune(5000)`。
- 证据：表名、文件数、SQL 文本中的过滤条件位置。
- 建议：REWRITE 补分区谓词/检查谓词写在 join 后未下推/核对 DPP 生效条件。明确标注为启发式（计划文本解析脆弱，见设计文档 §9.4），不给 CRITICAL。

**S-20 AQE 干预提示**（INFO）
- 触发：最终计划含 AQEShuffleRead 的 `number of skewed partitions > 0` 或 coalesce 生效、或 `plan_hash_initial ≠ plan_hash_final`。
- 用途：告诉使用者"AQE 已自动处理了 X 个倾斜分区/合并了 Y 个分区/把 SMJ 转成了 BHJ"——避免重复调优，也是 S-01/S-03 建议措辞的输入（"AQE 已介入但仍倾斜 → 调 factor" vs "AQE 未介入 → 先开开关"）。

**S-25 危险 Join 算子**（新；依赖 PLAN_METRICS + PLAN_TEXT）
- 动机：CartesianProduct（笛卡尔积）与 BroadcastNestedLoopJoin（BNLJ）是共享队列里最容易"一条拖垮全场"的算子——常因漏写 join 条件或非等值 join 意外触发。
- 触发：最终计划出现 `CartesianProduct`，或 `BroadcastNestedLoopJoin` 且其输出行数 `≥ T_bnlj_rows(1e8)` 或所在 stage 耗时占关键路径 `≥ T_dangerous_ratio(0.3)`。
- 证据：算子类型、join 两侧行数、输出行数、SQL 文本中的 join 条件（提示疑似缺失等值条件的位置）、关键路径占比。
- 建议：REWRITE 补等值 join 条件 / 改写非等值逻辑（如区间 join 用分桶或范围键）；CartesianProduct 单独 CRITICAL（几乎总是 bug 或严重反模式）。
- 层级：REWRITE。

**S-26 Join / Explode 行数放大**（新；依赖 PLAN_METRICS）
- 动机：多对多 join 或 explode 导致中间行数爆炸，是 spill/OOM 的上游根因；直接看行数放大比比看下游 spill 更早、更准。
- 触发：某 join/generate 节点 `output rows / max(左输入 rows, 右输入 rows, 1) ≥ T_row_amp(10)` 且 `output rows ≥ T_amp_min_rows(1e8)`。
- 证据：放大节点位置、输入/输出行数、放大比、下游是否随即 spill（S-07 联动）或倾斜（S-01 联动）。
- 建议：REWRITE 检查 join key 重复度（去重/预聚合）、explode 后尽早过滤/聚合；若为维度膨胀，考虑广播小维表改变 join 策略。
- 层级：REWRITE。

**S-29 Codegen 失效热点**（新，启发式，WARN 上限；依赖 PLAN_TEXT）
- 动机：whole-stage codegen 未生效的算子走解释执行，CPU 效率骤降；在 CPU-bound stage 上是隐蔽的性能损失。
- 触发：计划文本中关键路径 stage 存在未被 `*(n)` codegen 包裹的重算子（如某些 UDF、特定聚合），且该 stage `cpu.sum/run_time.sum ≥ T_cg_cpu(0.7)`（CPU-bound，S-10 联动）且耗时占关键路径 `≥ T_cg_ratio(0.2)`。
- 证据：未 codegen 的算子名、该 stage CPU 占比与耗时占比、疑似阻断 codegen 的算子（如非 codegen 友好的 UDF）。
- 建议：REWRITE 用内建函数替换阻断 codegen 的 UDF / 拆分复杂表达式；明确标注启发式（计划文本解析，见 §9.4），不给 CRITICAL。
- 层级：REWRITE。

**S-30 LIMIT 初始探测分区过少**（SESSION_SET，依赖 LIMIT_PROBE_METRICS）

- **触发：实际 LIMIT 取数轮次 > 2**；不再增加耗时、占比或至少扫描多少分区等门槛。
- 输入：当前初始分区数 `I`、实际累计探测分区数 `P`、日志记录的 `scaleUpFactor`（有效值 `F=max(2,配置值)`）、同时在场的 Executor 核数之和 `C`。
- **建议值：`min(C, max(I+1, ceil(P/(1+F))))`**。`I×(1+F)` 是简单的两轮探测分区预算，`P` 超出此预算时按本次读取规模反推初始值；即使未超出预算，只要实际超过两轮也提示调大。此公式是试验起点，不保证两轮完成或必然加速，不同时修改 scaleUpFactor。
- 例：批量 `1、4、20`，`P=25,F=4,C=32`，建议 `I=5`；批量 `1、4、20、100`，`P=125,F=4,C=16`，建议 `I=16`。
- 参数从目标 Job 的属性取实际值，跨轮不一致时不建议。初始值缺失时，可根据已确认取数序列的首批分区数推断并标注来源；scaleUpFactor 缺失时不假设默认值，改用第二轮实际批量作为候选，即 `min(C,max(I+1,第二轮分区数))`。
- `C` 来自 ExecutorAdded/Removed 维护的在场核数，不用 Task 数、历史累计核数或默认 1 替代；动态变化时取各轮开始所见核数的最小值。无法取得核数时只提示复核，不给数值；`I>=C` 时说明已达上限，不输出更大值或反向降低建议。
- 轮次按同一 execution、同一终端 RDD 的串行成功 Job 识别，首批与 `I` 一致，累计不超过 RDD 分区数；只对可识别的简单 CollectLimit 取数路径启用。不把 Shuffle、Join、子查询、重试或未完成的 Job 数当轮次。Task 完整性复用现有聚合计数，不新增 Task 明细。
- 输出：S-30 finding 携带轮次、累计分区、配置及来源、核数上限、候选值；有候选时 WARN/MEDIUM，无可增加空间或核数未知时 INFO。CLI、Live、HTML/JSON 使用现有 Finding/Recommendation 链路。
- 验证：2/3 轮边界、不同 F、F 下限、缺失配置、核数上限与动态变化、重试/错误归属/不完整任务反例；结束 SQL 在运行中 Application 内仍可评估。

Spark 的下一批也受已返回行数影响，`1+F` 仅用于上述启发式预算，不能解释为所有执行的固定增长轨迹。[Spark 3.5.6 executeTake 源码](https://github.com/apache/spark/blob/v3.5.6/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkPlan.scala)

### 3.F 稳定性

**S-21 Task 失败画像** ⚑
- 触发：execution 内 `failed_tasks > 0` 或 stage retry > 0。
- 分类（由 Task End Reason / error 文本映射）：
  - **OOM** → 联动 S-07/S-09/Q-14；
  - **FetchFailed**（含来源 BlockManager host）→ 喂给 Q-05，并汇入 **Q-13** src×dst 矩阵；
  - **Killed**（speculative 另计，联动 S-28）；
  - **ExceptionFailure**（取异常类名 TopN）；
  - **ExecutorLost** → 联动 Q-06；
  - **AUTH**（新增分类）：异常文本含 Kerberos/`GSS`/`delegation token`/`token (...) is expired` 等 → 认证/凭据失效。**长驻应用特有风险**：delegation token 到期未续导致批量 task 失败，往往集中在运行数小时后。
- 证据：失败 task 全明细（含 stderr URL）、重试放大系数 = 实际 task attempts / num_tasks、失败的时间聚集性（AUTH 类常呈"某时刻后集中爆发"）。
- 建议：按类映射（OOM→S-07/S-09 路径；FetchFailed→Q-05/Q-13；反复 speculative kill→S-28 评估；**AUTH→Q-16 的凭据续期分支**，检查 keytab/token 续期配置，这是 RESTART_CONF/平台侧）。

**S-28 推测执行效果评估**（新）
- 动机：speculation 在共享队列里是双刃剑——能救慢节点，也可能因大量无效重复浪费核。需量化它到底帮了还是亏了。
- 触发：全天存在 speculative task（`killed_speculative` 求和 > 0 或推测 task 数 ≥ T_spec_min(50)）。
- 证据：推测 task 数、其中"推测赢"（原 task 被 kill、推测副本更快完成）与"推测输"（推测副本被 kill 或更慢）的比例、推测消耗的额外 core-seconds、触发推测的 stage 是否集中在某 host（→ 与 Q-05 印证，说明推测在补偿坏节点）。
- 建议：推测多为"输" → RESTART_CONF 调高 `spark.speculation.multiplier`/`quantile` 或关闭；推测多为"赢"且集中单 host → 治本是修坏节点（Q-05），推测只是止血。
- 层级：RESTART_CONF。

### 3.G 基线

**S-22 指纹回归与计划漂移** ⚑（依赖 BASELINE）
- 触发：当日某指纹 `duration.p50 ≥ max(T_reg_ratio(1.5) × baseline.p50, baseline.p50 + T_reg_abs(5min))`，样本数 ≥ T_reg_n(3)。
- 五步归因链（自动执行，报告呈现为对照表，逐层排除）：
  1. `input_bytes` 涨了？→ 数据量增长（治本在上游）；
  2. `shuffle_read` 涨了？→ 中间量放大（联动 S-26）；
  3. 新增倾斜？→ skew_ratio 变化（联动 S-01）；
  4. `dominant_plan_hash` 变了？→ **计划漂移**（如 BHJ 退化为 SMJ——给出两版计划 diff 链接，最常见的静默回归来源，单独 CRITICAL）；
  5. queue_ctx 变了？→ 其实是排队恶化（转 S-14 结论，非 SQL 自身回归）。
- 建议：按命中的那一层分派到对应规则的建议；计划漂移强调统计信息/参数变化排查。
- 交叉引用：若同期多个指纹一起变慢，可能是全队列普涨而非个体回归，由 **Q-18** 区分（避免把平台抖动误判为逐条 SQL 回归）。

---
## 4. Q 系列：队列级规则

评估对象为一整轮（一个 Application 生命周期，约 22h）。Q 规则消费 occupancy 时间线、executor 拓扑、queue_summary 与全量 sql_executions/stages 聚合，回答"整个队列今天健康吗、瓶颈在哪、谁挤占了谁"。这些结论同时反哺 S 系列的 queue_ctx（S-14 尤其依赖）。

### 4.A 排队、容量与调度

**Q-01 队列占用时间线** ★⚑
- 产出（恒输出）：以基础桶（默认 10s，`occupancy.bucket_seconds`）聚合、按报告粒度（默认 **5min，可配** `report.occupancy_granularity`）上卷的全天曲线组——`running_tasks_avg/max`、`busy_ratio`、`tasks_submitted/completed/failed`、`active_statements`、shuffle/input/spill 字节流量、GC 总量。
- 这是用户"整个队列 Task 数量按 5 分钟粒度统计"需求的直接实现；任何单 SQL 报告都引用同一时间线切片（S-14 的 queue_ctx 即来源于此）。报告页内可切 1min/5min/15min，由内嵌基础桶前端上卷。

**Q-02 容量利用率与空闲/过载时段** ⚑
- 指标：`utilization = Σtask run_time / Σalive_core_seconds`（全天）；识别连续 ≥ T_win(30min) 的空闲窗（`busy_ratio < T_idle(0.2)`，固定 executor 即纯浪费）与过载窗（`busy_ratio > T_overload(0.95)` 且持续排队）。
- 建议：给出量化陈述（"全天利用率 43%，02:00–06:00 近乎空闲，09:30–11:00 持续打满且累计排队 4.2 core·h"），供容量决策（缩容/错峰/为过载窗单独扩队列）。
- 层级：RESTART_CONF / 运维策略。

**Q-03 排队热点时段 + 受影响 SQL 清单**
- 算法：对每个报告粒度桶，累计桶内所有 execution 的 queue_wait 投影，得"排队损失曲线"；取 TopN 热点窗，列出窗内 wait_ratio 最高的 SQL（statement_id、等了多久、被谁挤占——引用 top_consumers）。
- 价值：把 S-14 的单条归因上卷为管理视角："每天 09:30–10:00 平均 14 条 SQL 在排队，主要被指纹 X（大 ETL）挤占" → 错峰建议有名有姓。

**Q-04 单语句垄断**
- 触发：某 statement 连续 ≥ T_mono_win(10min) 占用 ≥ T_mono_ratio(60%) alive cores，且期间 `active_statements ≥ T_mono_concurrent(3)`（有人在等才算垄断）。
- 建议：SESSION/网关侧限流；RESTART_CONF 评估 FAIR 调度池（`spark.scheduler.mode=FAIR` + pool 配置，长驻共享队列默认 FIFO 时的经典改造点），报告附当前调度器配置现状。

**Q-11 系统性慢时段**
- 算法：对每个报告粒度桶计算"跨语句归一化 task 时长中位数" `median(norm)`（norm = task_duration / 所属 stage_p50，消除 stage 间差异）；某窗口 `median(norm) ≥ T_sys(1.4)` 且 `active_statements ≥ 3` 且非单 host 贡献（排除 Q-05 情形）→ 判定该时段系统性变慢（HDFS/网络/共置负载）。
- 证据：窗口内各语句均匀变慢的分布图、与 shuffle/scan 流量峰的相关性提示。
- 价值：避免把平台层抖动误判为 SQL 回归（S-22 求值时引用本规则结果做排除；Q-18 亦联动）。

### 4.B 稳定性与节点

**Q-05 慢节点 / 慢 Executor**
- 算法：对每个 task 计算 `norm = task_duration / stage_p50`（消除 stage 间差异）；按 host 聚合 `host_score = median(norm)`；触发：`host_score ≥ T_host(1.5)` 且样本 ≥ T_host_n(50) 且覆盖 ≥ T_host_stages(3) 个不同 stage。FetchFailed 的来源 host 计数、executor 级 GC 占比离群作为并列证据。
- 证据：host 排名表、该 host 上的 top 慢 task（含 stderr URL）、时间分布（全天慢 vs 某时段慢——后者可能是共置作业干扰）。
- 建议：平台侧排查（报告明确这是 event log 内证据的边界）；短期 RESTART_CONF exclude 该节点（`spark.excludeOnFailure.*` 现状对照）。
- 交叉引用：慢的成因若在磁盘/网络，分别见 **Q-12**（IO 吞吐）/ **Q-13**（网络矩阵）；三者交叉定位单机问题。

**Q-06 Executor 异常与丢失**
- 触发：出现 `ExecutorRemoved`（固定 executor 队列中任何非计划移除都异常）。
- 证据：removed reason 原文、时间点、丢失时受影响的 execution 列表（该时刻活跃 SQL）、随后的 stage retry 放大。
- 建议：reason 含 OOM/heartbeat → RESTART_CONF 内存或超时参数；关联 Q-05；频繁发生升级为 **Q-16** 风暴视角。

### 4.C 基线、配置与榜单

**Q-07 全天资源健康趋势（重启周期评估）**（依赖 STAGE_EXECUTOR_METRICS）
- 算法：对 executor 与 driver 的内存峰值、GC 占比按小时序列做趋势检验（线性斜率 + 首末四分位对比）。
- 输出："JVM 老年代峰值从 02:00 的 41% 爬升至 23:00 的 88%，支持维持每日重启"或"全天平稳，重启周期可放宽评估"——为 01:52 重启这一运维动作提供数据依据。
- 层级：RESTART_CONF / 运维策略。

**Q-08 静态配置体检**
- 对 conf.json 跑核对清单（内置 + 可扩展），逐项输出 现状/建议/理由，例如：AQE 全家桶开关、`advisoryPartitionSizeInBytes`、序列化器 Kryo、`spark.eventLog.rolling.*` 与 `logStageExecutorMetrics`（§2.3 自举，缺失会削弱本工具自身的诊断能力）、shuffle service、`spark.scheduler.mode`、speculation、`spark.locality.wait`、`maxResultSize`、listenerbus capacity。
- 全部 RESTART_CONF 层级，构成"次日重启窗口变更候选集"。

**Q-09 吞吐与 TopN 榜单** ⚑（INFO，日报骨架）
- SQL 总数/成功率、按小时吞吐、duration p50/p90 分布；榜单：Top-20 最慢 execution、Top-20 资源消耗（task_time_sum）、Top-20 排队受害者、失败 TopN（按 error_class×指纹）、指纹联赛表（次数×p50×资源，识别"高频中慢查询"这类总量大户）。

**Q-10 小文件全局治理清单**
- S-05/S-06 命中按 表名×指标 聚合去重，输出每日治理清单（表、日均产生小文件数、涉及指纹与负责人可注入的备注字段），GOVERNANCE 层级——单条 SQL 报告解决不了的系统性问题在这里收口。
- 交叉引用：写侧吞吐若同时异常见 S-23/Q-12。

---
### 4.D 生产故障域强化（新增）

**Q-12 节点 IO 吞吐异常**（新）
- 动机：把散落在各 stage 的磁盘信号（S-07 spill 落盘、S-23 shuffle 写慢）按 **host 聚合**，定位到具体坏盘/满盘节点——单条 SQL 看不出的节点级 IO 热点在这里现形。
- 算法：对每 host 汇总 shuffle 写吞吐（`Σshuffle_write_bytes / Σshuffle_write_ms`）、spill 落盘量、input 读吞吐；与全集群中位数比。触发：某 host 写吞吐 `≤ T_io_host(0.4) × median(所有 host)` 且该 host 样本 ≥ T_io_min_tasks(100)；或该 host spill 落盘量占全集群 `≥ T_io_spill_share(0.3)` 而其 core 占比远低于此。
- 证据：host×IO 指标榜（写吞吐/读吞吐/spill 量）、该 host 承载的 task 数与占比、时间分布（持续 vs 突发）、是否同时命中 Q-05（慢）与 S-23（写慢）。
- 建议：单 host 显著劣化 → 疑似坏盘/满盘/介质老化，平台侧换盘或排查（明确 event log 证据边界）；短期 RESTART_CONF exclude。
- 层级：平台侧 / RESTART_CONF。交叉引用：Q-05×Q-12 交叉矩阵（§6）区分"慢是因为盘"还是"慢是因为 CPU/其他"。

**Q-13 Shuffle 网络异常矩阵**（新）
- 动机：FetchFailed 与 fetch 慢的根因可能是单机故障、整机架问题或网络拥塞。把 fetch 事件组织成 **源 host × 目的 host 矩阵**，用分布形态区分三者——单条 S-11 只能看到"我拉取慢"，看不出"慢在哪段链路"。
- 算法：聚合 FetchFailed 与高 fetch_wait 的 (src_host, dst_host) 对，建矩阵。判定：
  - 某 **src 行**整体偏高（多个 dst 从同一 src 拉取都慢/失败）→ **单机故障**（该 src 节点或其 shuffle service 异常）；
  - 某 **机架内 src×dst 块**偏高 → **机架级**问题（交换机/机架网络）；
  - 矩阵**弥散偏高**且与 Q-01 shuffle 流量峰时间吻合 → **网络拥塞**（带宽饱和）。
- 证据：src×dst 热力矩阵、FetchFailed 计数 TopN 链路、与 shuffle 流量时间线的相关性、涉及的 stage/execution。
- 建议：单机 → 联动 Q-05/Q-06 exclude 或修复；机架 → 平台/网络团队；拥塞 → RESTART_CONF 调 `spark.reducer.maxSizeInFlight`/`maxReqsInFlight` 削峰 + 错峰调度。
- 层级：平台侧 / RESTART_CONF。数据来源：S-21 FetchFailed 分类（含来源 BlockManager host）汇入本矩阵。

**Q-14 内存不足综合诊断**（新；依赖 STAGE_EXECUTOR_METRICS 更佳）
- 动机：内存不足在共享队列有三种截然不同的成因，处方完全不同。把 S-07(spill)、S-08(GC)、S-09(峰值)、S-21(OOM)、Q-06(丢 executor) 的内存信号汇总，做**三分支归因**，避免"一律加内存"的粗糙处方。
- 三分支判定：
  - **单 SQL 黑洞**：内存压力（spill/OOM/GC 峰）时间上集中于某一条 statement 的执行窗，其余时段水位正常 → 是这条 SQL 的问题（行数放大 S-26 / 倾斜 S-01 / 该调分区）。处方：REWRITE/SESSION 针对该 SQL，**不动全局内存**。
  - **水位不足**：全天多数时段 executor 堆峰值持续逼近 Xmx（S-09 风险档普遍命中），非单条引起 → 集群内存配比确实偏低。处方：RESTART_CONF 提高 `spark.executor.memory` 或降 cores。
  - **并发叠加**：单条都不过分，但高并发窗（active_statements 高，Q-04/Q-03 时段）多条中等内存 SQL 叠加触顶 → 是调度/并发问题不是单条问题。处方：RESTART_CONF FAIR pool 限并发 + 错峰，而非无脑加内存。
- 证据：内存压力事件时间线（叠加 active_statements）、按 statement 的内存贡献归集、executor 峰值全天曲线、三分支的判定依据与置信度。
- 层级：视分支为 REWRITE / RESTART_CONF。

**Q-15 Driver 瓶颈画像（SPOF）**（新；依赖 STAGE_EXECUTOR_METRICS 更佳）
- 动机：单 Driver 是长驻共享队列的单点。它可能在四个维度成为瓶颈，任一都拖慢全场，却分散在各规则里。本规则做 Driver 专项体检。
- 四维度聚合：
  1. **Driver 内存/GC**：driver 堆峰值、GC 占比趋势（Q-07 的 driver 条目）；
  2. **事件积压**：全队列 scheduler_delay 系统性偏高（S-13 上卷）、丢事件迹象（DQ-01）指向 listenerbus 饱和；
  3. **规划间隙**：全队列 driver_gap 占比分布（S-15 上卷）、大量文件列举/AQE 重规划；
  4. **结果回传**：大 result_size 的 SQL 频次（S-12 c 分支上卷）、与 driver 内存峰吻合。
- 触发：任一维度越过其阈值（`driver_gc_ratio ≥ T_dgc(0.15)` / 全队列 `median(sched_delay) ≥ T_dsd(1s)` / 全队列 `median(driver_gap_ratio) ≥ T_dgap(0.25)` / 大结果 SQL 数 ≥ T_dresult_n(10)）。
- 证据：四维度评分卡、各维度贡献最大的 top SQL、driver 全天资源曲线。
- 建议：RESTART_CONF driver 核数/内存、listenerbus capacity、`maxResultSize`；规划间隙类联动 GOVERNANCE 分区治理。
- 层级：RESTART_CONF。

**Q-16 失败 / 重试风暴**（新）
- 动机：零星失败（S-21 单 SQL 视角）与"某时段大面积失败"是两回事。后者常由 executor 批量丢失、坏节点雪崩、或**认证凭据集中过期**引起，需要队列级时间聚集视角。
- 触发：某报告粒度窗内 `tasks_failed / tasks_submitted ≥ T_storm_ratio(0.1)` 且失败 task 绝对数 ≥ T_storm_abs(200)；或窗内 stage retry 数 ≥ T_storm_retry(20)。
- 分类（沿用 S-21 分类上卷）：
  - **节点雪崩**：失败集中于少数 host（联动 Q-05/Q-06/Q-13）；
  - **资源型**：OOM/ExecutorLost 为主（联动 Q-14）；
  - **AUTH 凭据风暴**（长驻应用特有）：失败集中在运行数小时后、异常文本为 Kerberos/delegation token 过期，且**跨 host 弥散**（不是单机）——典型特征是"某时刻起所有节点齐刷刷失败"。处方：检查 keytab 长期续期 / token renewal 配置（`--keytab`/`spark.kerberos.*`），这类问题不修则每轮固定时刻复发。
- 证据：失败率时间曲线（叠加 executor 增删事件）、失败类型构成、host 分布（判雪崩 vs 弥散）、首次爆发时间点（AUTH 类的判别关键）。
- 建议：按分类分派；AUTH → RESTART_CONF/平台侧凭据续期（联动 S-21 AUTH 分支）。
- 层级：RESTART_CONF / 平台侧。**风暴窗内的性能类结论自动降级**（§6）：该时段的 S-01…S-13 命中标注"处于失败风暴窗，指标可能失真"。

**Q-17 重启窗口影响评估**（新）
- 动机：每日 01:52 强制重启会打断在途 SQL。需要量化"重启砍掉了什么、有没有值得改的提交习惯"，并为"是否需要优雅 drain"提供依据。
- 算法：以 app 生命周期边界（ApplicationEnd / 下一轮 ApplicationStart）为界，识别**在重启时刻仍未完成**的 execution（有 start 无 end，或 end 状态为 CANCELLED 且时间贴近重启点）；统计轮末 T_restart_tail(30min) 内新提交的 SQL 数与它们的完成情况。
- 证据：被打断的 SQL 清单（statement_id、已运行时长、预计还需多久——按同指纹基线 p50 估算）、轮末高风险提交统计（临近重启才提交的大 SQL）、被打断 SQL 的资源浪费（已消耗 core-seconds 白费）。
- 建议：大量长 SQL 被砍 → GOVERNANCE/调度建议：轮末窗口对超长 SQL 提交做准入控制，或引入优雅 drain（重启前停止接新、等在途完成）；某些指纹反复被打断 → 建议错峰到轮初。
- 层级：GOVERNANCE / 运维策略。

### 4.E 队列级基线

**Q-18 队列级基线漂移**（新；依赖 BASELINE）
- 动机：S-22 判**单个指纹**回归。但当"今天整体变慢"时，需要区分是**个别 SQL 回归**还是**全队列普涨**（数据总量增长、集群降级、并发上升）——否则会把平台问题误判成一堆逐条 SQL 回归，开错药方。
- 算法：对队列级聚合指标做跨轮基线对比：当日 数据总量（Σinput_bytes）、全局利用率、全局 duration p50、成功率、总 task 数 vs 近 N 轮 rollup。判定：
  - 若**全局 p50 普涨**且回归指纹数占比高（大量指纹一起变慢）→ **系统性普涨**，指向数据增长（Σinput_bytes 同步涨）或集群降级（利用率涨但吞吐没涨，联动 Q-11/Q-05/Q-12）；
  - 若全局 p50 基本平稳但**少数指纹显著回归** → 确认是**个体回归**，交回 S-22 逐条归因。
- 证据：队列级指标的跨轮趋势卡（数据总量/利用率/全局 p50/成功率）、回归指纹数量与占比、普涨 vs 个体的判定依据。
- 建议：系统性普涨 → 数据增长则容量规划（Q-02 联动），集群降级则平台排查（Q-05/Q-12/Q-13）；个体 → S-22。
- 层级：视结论为 RESTART_CONF / GOVERNANCE / 平台侧。

---

## 5. DQ 系列：数据质量规则

DQ 规则评估**解析结果本身的可信度**，不参与 score 排序，作为全局置信度声明置于报告"解析质量"章节。它们的意义是：在任何性能结论之前，先诚实声明"这份数据有多可信、哪些结论要打折"。

**DQ-01 事件完整性**
- 触发：解析过程中检测到丢事件迹象——某 stage `task_end_received < num_tasks`（标记 `metrics_partial`）、或遇到 `*.compact` 文件（SHS compaction 已丢 Task 级明细，设计文档 §3.1）、或 `.inprogress` 截断、或非白名单事件计数异常高。
- 证据：partial stage 数与占比、丢失事件类型计数、compaction/inprogress 状态、受影响的 execution 列表。
- 影响声明：partial stage 上的分位数类规则（S-01/07/08/11…）输出封顶 WARN 并标注置信度；受影响 execution 的资源类结论标注"可能偏低（部分 task 指标缺失）"。
- 根治建议：Driver 事件队列溢出 → Q-08/Q-15 的 listenerbus capacity；compaction 冲突 → 确保 02:05 解析先于 compaction 或对本应用关闭 compaction。
- 层级：观测自身（RESTART_CONF 根治）。

**DQ-02 时间一致性**
- 触发：检测到时钟异常——task `finish_ts < launch_ts`（负时长，截断为 0 但计数）、executor 事件时间与 task 时间跨度矛盾、不同 host 的时间戳呈现系统性偏移（同一 stage 内 launch/finish 分布异常）。
- 证据：负时长 task 计数、疑似时钟偏移的 host 列表与偏移量估计、受影响的 occupancy 桶。
- 影响声明：**时钟偏移会污染以 host 为单位的结论**——Q-05/Q-12/Q-13 的 host 级判定在检测到偏移时降级（标注"该 host 时间戳可疑，慢/异常结论存疑"）；occupancy 时间线在偏移窗标注不确定。
- 根治建议：平台侧 NTP 校时（明确这是 event log 外的运维项）。
- 层级：观测自身 / 平台侧。

---
## 6. 规则联动关系

规则不是孤立的。三类联动关系必须在引擎中显式实现，否则会产出互相矛盾或误导的结论。

### 6.1 汇总联动（单 SQL 命中 → 队列级收口）

| 上游（S 系列命中） | 下游（Q 系列汇总） | 汇总方式 |
|---|---|---|
| S-05 扫描小文件 · S-06 输出小文件 | Q-10 小文件全局治理 | 按 表名×指标 去重聚合 |
| S-23 Shuffle 写吞吐异常 | Q-12 节点 IO 吞吐 | 按 host 聚合磁盘信号 |
| S-11 Fetch Wait · S-21 FetchFailed 分类 | Q-13 Shuffle 网络矩阵 | 按 (src,dst) host 对建矩阵 |
| S-07 · S-08 · S-09 · S-21(OOM) · Q-06 | Q-14 内存不足综合诊断 | 三分支归因（单 SQL/水位/并发） |
| S-13 · S-15 · S-12(c 分支) | Q-15 Driver 瓶颈画像 | 四维度上卷 |
| S-22 单指纹回归 | Q-18 队列级基线漂移 | 回归指纹数占比 → 个体 vs 普涨 |

### 6.2 降级联动（可信度不足 → 抑制或弱化结论）

- **Q-16 失败/重试风暴窗** → 窗内所有 S-01…S-13 性能类命中标注"处于失败风暴窗，指标可能失真"，不进报告头部。理由：大面积重试会污染 task 时长/资源指标，此时谈单条性能没有意义。
- **DQ-01 事件完整性**（partial stage）→ 该 stage 的分位数类规则（S-01/02/07/08/11…）封顶 WARN 并标注置信度；资源类结论标注"可能偏低"。
- **DQ-02 时间一致性**（时钟偏移）→ **所有以 host 为单位的结论降级**：Q-05/Q-12/Q-13 在偏移 host 上的判定标注"时间戳可疑，结论存疑"；occupancy 时间线在偏移窗标注不确定。
- **Q-11 系统性慢时段** → S-22 求值时引用其结果，把落在系统性慢窗内的指纹回归标注"疑似平台层原因，非 SQL 自身"。

### 6.3 加权与仲裁联动（同一现象的多规则协同）

- **S-16 关键路径加权**：落在关键路径 stage 上的 S-17（广播机会）/S-25（危险 Join）/S-29（Codegen 失效）命中，severity 与 score 上调（乘关键路径占比因子）；不在关键路径上的同类命中降 INFO。理由：改非关键路径的东西不缩短总耗时。
- **S-14 仲裁 SELF vs QUEUE**：S-14 判为 SLOW_DUE_TO_QUEUE 时，该 execution 的 S-01…S-13 自身瓶颈命中整体降权（先解决排队）；判为 SELF 时才放行自身分析。
- **S-20 措辞输入**：S-01/S-03 的建议措辞依 S-20 结果分叉（"AQE 已介入但仍倾斜→调 factor" vs "AQE 未介入→先开开关"）。
- **Q-05 × Q-12 交叉矩阵**：某 host 同时命中 Q-05（慢）与 Q-12（IO 差）→ 结论"慢因磁盘"，处方换盘；仅命中 Q-05 不命中 Q-12/Q-13 → 慢因 CPU/其他，另查。避免"节点慢"这一笼统结论。
- **S-28 × Q-05**：推测执行多为"赢"且集中单 host → 说明推测在补偿坏节点，治本是 Q-05 修节点而非调推测参数。
- **Q-14 三分支 × S-26**：Q-14 判"单 SQL 黑洞"时，优先指向该 SQL 的 S-26（行数放大）/S-01（倾斜）作为根因，而非全局加内存。

---

## 7. 默认阈值全表（conf.yaml）

运行时唯一阈值来源。规则代码只读符号名，此处是其默认值与单位的权威定义。按队列可差异化覆盖；字节一律 bytes、时长一律 ms。引擎启动时对"规则声明的阈值键 ↔ 本文件"做双向校验（缺键报错、冗余键告警）。

```yaml
thresholds:
  limit:         {max_rounds: 2}                                                               # S-30
  # A 数据分布与并行度
  skew:          {min_tasks: 20, abs_ms: 120000, ratio: 5, bytes_ratio: 8, bytes_abs: 1073741824}   # S-01/S-02
  partitions:    {many_tasks: 2000, tiny_ms: 2000, overhead_ratio: 0.3, huge_bytes: 536870912}       # S-03/S-04
  small_files:   {scan_files: 1000, scan_avg_bytes: 8388608,
                  out_files: 500, out_avg_bytes: 16777216,
                  prune_files: 5000}                                                                  # S-05/S-06/S-19
  # B 内存与 CPU
  spill:         {abs_bytes: 10737418240, ratio: 0.2}                                                 # S-07
  gc:            {warn: 0.10, crit: 0.20, min_runtime_ms: 600000}                                     # S-08
  memory:        {peak_risk_ratio: 0.9, peak_waste_ratio: 0.5, sizing_factor: 1.25}                   # S-09
  cpu:           {bound_ratio: 0.7, io_ratio: 0.3}                                                    # S-10
  # C Shuffle / 网络 / 磁盘
  fetch_wait:    {ratio: 0.15, min_shuffle_bytes: 1073741824}                                         # S-11
  serialization: {deser_p95_ms: 5000, result_ser_ratio: 0.1,
                  result_sum_bytes: 1073741824, result_task_bytes: 268435456}                         # S-12
  shuffle_write: {min_bytes: 10737418240, throughput_bytes_per_s: 52428800, task_ratio: 0.3}          # S-23
  locality:      {bad_ratio: 0.5, min_tasks: 200}                                                     # S-24
  # D 调度与关键路径
  scheduler_delay: {p50_ms: 1000}                                                                     # S-13
  queue:         {wait_ratio: 0.4, busy_ratio: 0.85}                                                  # S-14
  driver_gap:    {ratio: 0.3, min_duration_ms: 60000}                                                 # S-15
  fragmentation: {min_jobs: 50, job_median_ms: 3000, gap_ratio: 0.2}                                  # S-27
  # E 执行计划
  broadcast:     {opportunity_bytes: 67108864, risk_bytes: 536870912}                                 # S-17/S-18
  dangerous_join:{bnlj_rows: 100000000, critical_path_ratio: 0.3}                                     # S-25
  row_amp:       {ratio: 10, min_rows: 100000000}                                                     # S-26
  codegen:       {cpu_ratio: 0.7, critical_path_ratio: 0.2}                                           # S-29
  # F 稳定性
  speculation:   {min_tasks: 50}                                                                      # S-28
  # G 基线
  regression:    {ratio: 1.5, abs_ms: 300000, min_samples: 3, baseline_rounds: 14}                    # S-22

  # ---- 队列级 ----
  host:          {score: 1.5, min_tasks: 50, min_stages: 3}                                           # Q-05
  capacity:      {idle_window_min: 30, idle_ratio: 0.2, overload_ratio: 0.95}                         # Q-02
  monopoly:      {window_min: 10, core_ratio: 0.6, min_concurrent: 3}                                 # Q-04
  systemic:      {norm_median: 1.4, min_concurrent: 3}                                                # Q-11
  io_hotspot:    {host_throughput_ratio: 0.4, min_tasks: 100, spill_share: 0.3}                       # Q-12
  net_matrix:    {min_fetchfail: 20, src_row_ratio: 0.5, rack_block_ratio: 0.5}                       # Q-13
  oom:           {waterline_ratio: 0.9, concurrent_statements: 3}                                     # Q-14（分支判定）
  driver_health: {gc_ratio: 0.15, sched_delay_ms: 1000, gap_ratio: 0.25, big_result_n: 10}            # Q-15
  storm:         {fail_ratio: 0.1, fail_abs: 200, retry_abs: 20}                                      # Q-16
  restart_win:   {tail_min: 30}                                                                       # Q-17
  queue_baseline:{global_p50_ratio: 1.3, regressed_fraction: 0.3, baseline_rounds: 14}                # Q-18
  # H 数据质量
  dq:            {partial_stage_warn: true, clock_skew_ms: 5000}                                       # DQ-01/DQ-02

occupancy:       {bucket_seconds: 10}                              # 基础桶粒度（Q-01 上卷源）
report:          {occupancy_granularity: 5min, top_slow_sql: 20, top_tasks_per_stage: 10,
                  render_policy: [top_slow, critical]}             # 报告默认渲染 top_slow + CRITICAL 命中

# 规则静默清单（§1.4），规则×指纹/语句/表 粒度
suppressions: []
  # 示例：
  # - {rule: S-05, table: "ods.legacy_dim", reason: "已排期 compaction", until: "2026-09-30"}
  # - {rule: Q-04, statement_id: "d3f1...", reason: "夜间大 ETL 业务已知"}
```

阈值调参原则：先按 §2.1 的 ⚑ 首批 12 条上线并观察 1–2 轮，用真实 findings 分布校准（噪声多则收紧、漏报则放宽）；`RESTART_CONF` 类建议因每日重启天然可 A/B，调参后对比前后两轮即可验证。

---

## 8. 规则生命周期

规则库是本工具"活"的部分，会持续演进。为保证 ID 稳定与结论可回溯，新增/修改规则遵循固定流程：

1. **提案**：描述动机（覆盖哪个 §2.2 故障域）、触发信号、依赖能力（§2.3）、预期建议层级。**LLM 层（阶段 4）的"规则未命中但确实慢"假设是新规则的主要来源**——LLM 反哺的模式经人工确认后进入提案。
2. **评审**：确认与既有规则无重叠、无冲突（尤其 §6 联动是否需要更新）；确认阈值可外置、有合理默认值。
3. **ID 分配**：取当前系列最大编号 +1（§1.1），与主题分组无关；一经分配不回收。
4. **Golden 用例（强制）**：每条新规则必须附至少一个 golden 用例——构造最小 event log fixture 或 MetricsContext，断言"该输入 → 该 finding（含 severity/证据字段）"。可复现性即测试（设计文档 §8）：没有 golden 用例的规则不予合入。
5. **阈值登记**：在 §7 conf.yaml 登记默认值与单位；引擎启动校验会拒绝"声明了阈值键但配置缺失"的规则。
6. **上线观察**：新规则默认以较保守阈值上线，观察 1–2 轮 findings 分布后校准；必要时先置为 INFO 累积样本，再按数据提升 severity 档位。

修改现有规则时：改阈值默认值 → 只动 §7 + conf.yaml，不改 ID、不改代码；改触发逻辑 → 更新本文档对应条目 + golden 用例同步更新；废弃 → 标 `DEPRECATED` 保留 ID，说明替代规则。

---

*（本文档为规则单一事实源。架构、解析层、指标仓库 schema、报告结构、CLI、工程结构与里程碑见《SparkAdvisor 设计文档》。）*
