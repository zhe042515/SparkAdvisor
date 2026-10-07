# SparkAdvisor

SparkAdvisor 是一个以 **Java 8 运行时兼容** 为目标、面向 **Apache Spark 3.5.6** 的 Event Log 离线分析与调优顾问。它读取 HDFS 上归档的 Spark event log，复用 Spark 自带的 `ReplayListenerBus` / `JsonProtocol` 还原事件，计算关键路径与硬指标，基于规则和成本模型给出调优建议，并输出自包含 HTML 或 JSON 报告。除单条 SQL 诊断外，项目还提供 `sparkadvisor-monitor` 队列分析层，用于长驻查询队列的一整轮跨 SQL 聚合分析。

完整设计见 [SparkAdvisor 设计文档](docs/SparkAdvisor设计文档.md)，仓库协作与实现约束见 [AGENTS.md](AGENTS.md)。

## 核心能力

- **离线解析 event log**：支持单文件、rolling event log 目录、压缩日志与 `.inprogress` 不完整日志。
- **StatementID 定位 SQL**：通过 SQL 开头的 `/* StatementID */` 注释定位目标语句；若输入纯数字且没有 StatementID 命中，则回退按 `executionId` 定位。
- **LIMIT 初始分区建议（S-30）**：实际取数超过 2 轮时，结合累计探测分区数与 `scaleUpFactor` 建议调大 `spark.sql.limit.initialNumPartitions`，候选不超过实际 Executor 总核数；Live 与 CLI 共用。完整规则见 [rules.md](docs/rules.md)。
- **硬指标分析**：计算关键路径、理想耗时、实际墙钟、核心利用率、倾斜比、spill、GC 占比、Stage 启动/资源等待、shuffle fetch wait 与 task attempt 等指标。
- **规则诊断**：按 [docs/rules.md](docs/rules.md) 注册 50 条稳定规则：S 系列 30 条、Q 系列 18 条、DQ 系列 2 条。规则通过 `MetricsContext` 与 capability 声明消费聚合指标；缺少 plan metrics、executor 峰值、host/network 矩阵或 baseline 时显式跳过，绝不把缺失值当成 0 触发。旧 R1-R11 引擎仅保留为迁移兼容入口。
- **调参预测**：对 shuffle partition 与 executor/core 伸缩做成本模型估计，输出假设、置信度和可能反转条件；倾斜场景会明确提示“调分区通常无效”。
- **Advisor 建议**：默认使用离线确定性的 `RuleBasedAdvisor`；可通过 `--advise llm` 调用 LLM Advisor，默认 Provider 为 MiniMax-M2.5。LLM 只消费结构化 `AnalysisResult` / `QueueAnalysisResult` JSON，绝不发送原始 event log。
- **队列级监控分析**：`queue-report` 汇总一整个长驻应用的所有 SQL，输出延迟分位、瓶颈聚类、固定资源池利用率、争用受限查询、资源大户与全局调参建议。
- **Spark UI 集成**：提供 Spark History Server tab 插件，通过 `AppHistoryServerPlugin` / `ServiceLoader` 接入；同时提供 live Driver Spark UI 插件，通过 `spark.plugins` 接入运行中查询队列。

## 架构原则

SparkAdvisor 的唯一数据契约是 `AnalysisResult` JSON。CLI、HTML 报告、History Server tab 和 Advisor 都只消费这一契约，不绕过它直接读取领域模型。
队列级分析使用平级契约 `QueueAnalysisResult` JSON，同样不暴露 Spark 类型。

```text
HDFS Event Log
  -> sparkadvisor-core      读取、回放、领域模型、StatementID 定位、指标聚合与关键路径
  -> sparkadvisor-analyzer  RuleEngine，产出 Finding 与 Recommendation
  -> sparkadvisor-predictor shuffle/executor 成本模型预测
  -> sparkadvisor-report    AnalysisResult JSON 与自包含 HTML 渲染
  -> sparkadvisor-advisor   RuleBasedAdvisor / LlmAdvisor
  -> sparkadvisor-monitor   QueueAnalysisResult、争用时间轴、队列级规则与报告
  -> sparkadvisor-cli       命令行入口
  -> sparkadvisor-ui-plugin Spark History Server tab / live Driver tab
```

关键约束：

- 生产源码按 Java 8 兼容编写，使用 JDK 21 编译为 Java 8 bytecode；测试源码可使用 JDK 21 语法。不使用 Scala 编写源码。
- Spark/Hadoop 依赖全部是 `provided`，不会打进 CLI 或插件 fat-jar；运行时由集群 `/opt/client` 环境提供 classpath。
- 事件解析不手写 Spark event schema，统一复用 Spark 的回放机制，降低跨小版本字段演化风险。
- 解析必须流式、低内存；task 指标在 `onTaskEnd` 增量进入分位数估计器，默认不保留全部原始 task。队列争用分析会显式开启轻量 `TaskInterval` 收集，只保存 launch/finish/execution 归属。
- HTML 报告是单文件自包含输出，内联 CSS/SVG/JSON，不引入前端构建链。

## 当前状态

项目已完成 M1-M3、F4 与监控模块主体功能：

- **M1**：父 POM、`core/report/cli`、领域模型、分位数估计器、StatementID 提取与定位、event log reader/parser、`MetricAggregator`、`AnalysisResult`、HTML/JSON 报告、CLI `analyze` 端到端。
- **M2 / Rules v2**：`sparkadvisor-analyzer` 已实现 50 条 S/Q/DQ 稳定规则目录、外置阈值覆盖、capability gating、partial 降级、suppression 审计与旧 R1-R11 兼容入口；`sparkadvisor-predictor` 提供成本模型预测，报告包含 Predictions 区域。
- **M3**：History Server tab 插件已实现，采用自给自足策略重解析 event log，不依赖 SHS 内部 store；live Driver tab 通过 `SparkPlugin` 挂载，使用 driver listener 增量快照。
- **F4**：`TuningAdvisor`、`RuleBasedAdvisor`、`LlmAdvisor`、`MinimaxLlmProvider`（默认 MiniMax-M2.5）、`AnthropicLlmProvider`（可选）、prompt 构造与 LLM JSON 响应解析已实现；LLM 失败时优雅降级。
- **Monitor**：新增 `sparkadvisor-monitor`，包含 `QueueAnalyzer`、`QuerySeriesCollector`、`ContentionTimeline`、`QueueAggregator`、`QueueRuleEngine`、`QueueAnalysisResult`、队列 HTML/JSON 渲染和队列级 LLM Advisor；CLI 新增 `queue-report`，SHS tab 空 StatementID 时异步生成队列报告。

已验证项：

- 全模块已用 JDK 21 执行 `mvn -q clean package` / `mvn -q test` 通过，生产 class major version 验证为 52（Java 8）。
- analyzer、predictor、advisor、CoreTimeline、50 条规则 golden trigger/负例/capability/partial/suppression、LLM JSON 解析与端到端报告渲染相关测试已通过。
- monitor 的争用受限分类、瓶颈聚类、队列级建议、HTML/JSON 契约测试已通过。

生产集群首次部署前建议用目标 Spark 3.5.6 发行版再做一次 `mvn -q -DskipTests package`。触及 Spark 内部 UI/API 的代码仍保留 `// VERIFY@3.5.6` 标注，升级 Spark patch 版本时需复核。

## 构建

使用 JDK 21 构建，生产源码按 Java 8 release 编译：

```bash
mvn -q -DskipTests package
mvn -q test
```

构建产物：

- CLI jar：`sparkadvisor-cli/target/sparkadvisor-cli.jar`
- History Server 插件 jar：`sparkadvisor-ui-plugin/target/sparkadvisor-ui-plugin.jar`

Spark/Hadoop 依赖为 `provided`，运行时必须从集群 Spark/Hadoop classpath 提供。

## CLI 使用

在集群客户端节点上运行。推荐使用仓库内的启动脚本，它会加载集群环境、执行固定 Kerberos 初始化；在 JDK 9+ 运行时会自动添加 Spark 3.5.6 回放 event log 所需的 `--add-opens` 参数，Java 8 运行时不会添加。

```bash
bin/sparkadvisor analyze \
  --path hdfs:///spark2x/eventLog/application_1700000000000_0001 \
  --statement-id 20260521_abc123 \
  --advise rule \
  --format html \
  --out ./report.html
```

常用参数：

| 参数 | 说明 |
| --- | --- |
| `--path` | HDFS event log 路径，支持单文件或 rolling 目录。 |
| `--statement-id` | SQL 开头 `/* StatementID */` 中的 ID；纯数字可回退按 `executionId` 查找。 |
| `--format html\|json` | 输出格式，默认 `html`。 |
| `--out` | 输出文件路径，默认 `report.<format>`。 |
| `--top` | 未指定 StatementID 时，选择最慢的 N 条 SQL，当前 CLI 取其中最慢一条生成报告。 |
| `--keep-raw` | 调试用，保留原始 task 记录，会显著增加内存占用。 |
| `--hadoop-conf-dir` | 覆盖环境变量中的 Hadoop 配置目录。 |
| `--auth-to-local` | 覆盖 Hadoop `hadoop.security.auth_to_local` 规则；也可用环境变量 `SPARKADVISOR_AUTH_TO_LOCAL`。 |
| `--advise none\|rule\|llm` | Advisor 模式，默认 `rule`；`llm` 默认调用 MiniMax-M2.5，需要 `MINIMAX_API_KEY`。可用 `llm:claude` 走 Anthropic。 |
| `--lang auto\|zh\|en` | 报告语言，默认 `auto`；`auto` 下输出文件名包含 `_zh` 时生成中文，否则英文。 |
| `--rule-config` | 可选的规则阈值 YAML；格式与 `docs/rules.md` §7 的完整 `thresholds:` 区一致，缺键会失败。 |

`bin/sparkadvisor` 默认给 CLI 设置 `-Xmx4g`，用于覆盖 JDK 在容器/客户端节点上可能选择的较小默认堆，避免 Spark `JsonProtocol` 回放 100MB+ JSON event-log part 时 OOM。可用 `SPARKADVISOR_HEAP=8g` 调整，或用 `SPARKADVISOR_JAVA_OPTS="-Xmx8g -XX:+UseG1GC"` 直接追加 JVM 参数。

LLM 模式示例：

```bash
export MINIMAX_API_KEY=...
bin/sparkadvisor analyze \
  --path hdfs:///spark2x/eventLog/application_1700000000000_0001 \
  --statement-id 20260521_abc123 \
  --advise llm \
  --format html \
  --out ./report-llm_zh.html
```

`--advise llm` 只会发送结构化 `AnalysisResult`，不会发送 GB 级 raw event log。报告语言可用 `--lang zh|en` 显式指定；默认 `auto` 保留“HTML 输出文件名包含 `_zh` 时生成中文报告”的兼容行为。
可选环境变量：`MINIMAX_MODEL` 覆盖默认模型，`MINIMAX_BASE_URL` 指向内部网关或代理。

队列级历史报告：

```bash
bin/sparkadvisor queue-report \
  --path hdfs:///spark2x/eventLog/application_1700000000000_0001 \
  --format html \
  --out ./queue-report_zh.html \
  --top 50 \
  --sample-per-stratum 5 \
  --bucket 1h \
  --advise llm
```

`queue-report` 用于分析一个完整长驻查询队列应用的一整轮 event log。`--top` 控制深度分析的最慢 SQL 数量；`--sample-per-stratum` 控制 spill/fetch/GC/skew/template 等分层补样数量；其它 SQL 仍进入吞吐、延迟和趋势聚合。`--bucket` 支持 `15m`、`1h`、`3600s` 等形式。队列级 `--advise llm` 同样默认使用 MiniMax-M2.5，并只发送结构化 `QueueAnalysisResult`。报告语言同样支持 `--lang auto|zh|en`。

History Server 队列页也支持 `top`、`samplePerStratum`、`bucket` 参数，并把 rolling event-log snapshot key 与这些分析参数一起作为缓存/checkpoint key，避免不同抽样或分桶配置复用同一份报告。checkpoint 目录默认 `${java.io.tmpdir}/sparkadvisor-queue-checkpoints`，可用 `SPARKADVISOR_QUEUE_CHECKPOINT_DIR` 覆盖。当前 checkpoint 是结果 fast path，不是 byte-offset 增量回放；报告会以 `incremental=false` 和 `degradedReason` 保持诚实标注。

## History Server 插件

插件以 Spark History Server tab 的形式接入，部署文档见 [DEPLOY.md](DEPLOY.md)。

基本流程：

```bash
mvn -q -DskipTests -pl sparkadvisor-ui-plugin -am package
cp sparkadvisor-ui-plugin/target/sparkadvisor-ui-plugin.jar "$SPARK_HOME/jars/"
$SPARK_HOME/sbin/stop-history-server.sh
$SPARK_HOME/sbin/start-history-server.sh
```

打开 SHS 中任意应用后，导航栏会出现 **SparkAdvisor** tab。默认留空 StatementID 时展示该应用的队列级报告；输入 StatementID 后点击分析可下钻单条 SQL。

URL 形式：

```text
.../history/<appId>/sparkadvisor/?statementId=<ID>
```

插件采用“自给自足”集成策略：`createListeners` 返回空，不干预 SHS 自身回放；tab 打开时使用 SparkAdvisor 自己的引擎懒解析 event log。队列报告按 event log 快照大小/修改时间异步单飞缓存，避免在 SHS UI 请求线程同步解析大日志。插件异常会被捕获并记录，不应影响应用原有 History UI。

同一个 jar 也提供 live Driver Spark UI 入口。启动查询队列时把 jar 放入 driver classpath，并配置：

```bash
--conf spark.plugins=io.sparkadvisor.ui.live.SparkAdvisorSparkPlugin \
--conf spark.sparkadvisor.live.enabled=true
```

live 模式通过 driver listener 增量聚合快照，不重放 event log；单 SQL 诊断默认可用。Live 默认采用有界保留窗口，已完成 Stage 会释放 t-digest，SQL/Job/Stage/ExecutorEvent 和可选 TaskInterval 都有容量限制，适配 24 小时、千万 Task 的长驻应用。队列页如需资源占用/争用估计，可额外打开 `spark.sparkadvisor.live.collectTaskIntervals=true`；此时默认仅保留最近 10 万条 TaskInterval，完整全天分析仍使用 event log。

## 报告内容

HTML 报告包含：

- 应用概览与目标 SQL 概览
- 关键路径图：理想耗时、关键路径、实际墙钟三线对比
- 硬指标面板：倾斜、利用率、偏离度、spill、GC 等
- Findings：按严重程度排序的规则诊断
- Predictions：shuffle partition 与 executor/core 伸缩曲线
- Recommendations：SQL 改写与 Spark 配置建议
- Advisor 输出：规则版或 LLM 版摘要与建议
- 页面底部内嵌完整 `AnalysisResult` JSON
- `--lang zh|en` 可显式指定报告语言；默认 `auto` 下文件名包含 `_zh` 时输出中文 HTML，否则输出英文 HTML

示例报告见 [samples/demo-report.html](samples/demo-report.html)。

队列级 HTML 报告包含：

- 队列概览：应用、窗口、查询数、运行中 SQL 数、固定 core 数
- 延迟趋势：按时间桶的查询数、P50/P95/P99 与平均利用率
- 瓶颈聚类：慢查询 top-N 中反复出现的单 SQL 规则
- 争用报告：争用受限占比、热点时段、资源大户
- 慢查询榜：StatementID、executionId、耗时、主导瓶颈、争用分类
- 全局建议：Q-01–Q-18 队列级规则产出的证据、置信度和覆盖范围
- AI 队列建议：`queue-report --advise llm` 生成的队列级总结与建议
- 页面底部内嵌完整 `QueueAnalysisResult` JSON

## 运行环境注意事项

- CLI 约定在集群客户端节点运行，`bin/sparkadvisor` 会执行：
  - `source /opt/client/bigdata_env`
  - `kinit -kt /opt/client/keytab/ossuser.keytab ossuser`
  - 使用集群 Spark/Hadoop jar 作为运行时 classpath
- Spark 3.5.6 在 JDK 9+ 上回放日志可能需要若干 `--add-opens` 参数；启动脚本会按 JVM 版本条件添加，SHS 部署文档也列出配置方式。
- `.inprogress` 或被 compaction 的 rolling log 可能缺少尾部事件，SparkAdvisor 会标注 `incomplete=true`，相关预测置信度应按报告提示解读。
- 队列争用是基于 task 占用率的推断，event log 不直接记录排队等待；FAIR scheduler 或多 pool 场景下应降低归因置信度。
- AQE 开启时，有效分区数应以运行时 AQE 事件和最终计划为准；报告中的建议会区分 `shuffle.partitions`、`advisoryPartitionSizeInBytes` 与 skew join 相关参数。

## 后续可选优化

- predictor 对成本模型参数 `o` / `r` 做多点回归拟合。
- per-task 内存预算结合 `spark.memory.fraction` 做更精细估算。
- 增加本地模型 LLM provider。
