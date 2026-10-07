# AGENTS.md — SparkAdvisor

本文件为在本仓库工作的 Code Agent（Codex 等）提供工作约定。完整设计见 `docs/SparkAdvisor设计文档.md`。

当前在 `main` 沿用原架构和全部旧规则，新增 S-30 LIMIT 初始分区建议，规范见 `docs/rules.md`；不实施精简分支的架构。目标 Spark 版本改为 **3.5.6**，新接入 API 使用 `VERIFY@3.5.6` 并编译验证。下文 3.5.1 的已核对记录保留为历史参考，不能视为 3.5.6 的验证声明。

## 1. 项目一句话

SparkAdvisor：以 **Java 8 运行时兼容** 为目标、面向 **Spark 3.5.1** 的 Event Log 离线分析与调优顾问。读取 HDFS 上归档的 event log → 复用 Spark 自带 `ReplayListenerBus`/`JsonProtocol` 解析 → 计算关键路径与硬指标 → 规则预测与建议 → 输出 HTML/JSON 报告 → 可按 **StatementID** 在轻量页面查看结论。

## 2. 技术栈与硬约束（不可违反）

- **语言**：生产源码必须兼容 Java 8（`maven.compiler.release=8`），使用 JDK 21 编译；测试源码可用 JDK 21 语法（`maven.compiler.testRelease=21`）。**禁止用 Scala 编写**源码；但要在 JVM 上调用 Spark 的 Scala 产物（以 `provided` 依赖）。前端仅在报告/页面呈现时使用原生 HTML/CSS/JS，**不引入前端构建链**。
- **HTTP 客户端**：生产代码如需 HTTP 调用，使用 Apache HttpClient **4.5.14**，不得使用 Java 11 `java.net.http`。
- **Spark/Hadoop 依赖一律 `provided`**：编译期可见，**不打进 fat-jar**。运行时由 `/opt/client` 集群环境提供 Spark/Hadoop classpath。
- **解析不手写事件 schema**：必须复用 `org.apache.spark.scheduler.ReplayListenerBus` + 自定义 Java `SparkListener`。事件对象的还原交给 Spark 的 `JsonProtocol`（replay 内部完成）。理由：规避跨版本字段演化维护债。
- **流式、低内存**：单条复杂 SQL 的 event log 可达 GB 级。`onTaskEnd` 时**增量**喂分位数估计器（t-digest / 固定桶直方图），**绝不**保留全部原始 task（除非 `--keep-raw` 调试）。
- **唯一数据契约是 `AnalysisResult`（JSON）**：CLI、UI、未来的 LLM 顾问都只消费它。任何新接入方式都不得绕过这个契约去读领域模型。
- **诚实的预测**：硬指标（倾斜比/利用率/偏离度等）是精确实测值；“调参后变快/变慢”是**成本模型估计**，输出必须带假设 + 置信度（HIGH/MEDIUM/LOW），倾斜场景要明确告知“调分区可能无效”。

## 3. 编译验证

当前仓库应保持可在 JDK 21 下执行 Maven 编译验证，并产出 Java 8 bytecode。据此：

- 写代码时严格按下方“§4 已核对的 Spark 3.5.1 API”，不要臆造签名。
- 凡涉及 Spark 内部 API 的地方，在代码注释里标注 `// VERIFY@3.5.1`，提示首次在有 Maven 的环境编译时人工核对。
- 常规验证命令：`mvn -q clean package` 与 `mvn -q test`。涉及 Java 8 兼容性时抽查生产 class major version 为 52。

## 4. 已核对的 Spark 3.5.1 关键 API（依据官方源码 v3.5.x）

- `org.apache.spark.scheduler.ReplayListenerBus`：`private[spark]` 类，字节码 public，**Java 可 `new`**。Spark 3.5.1 字节码暴露的核心方法是
  `boolean replay(InputStream logData, String sourceName, boolean maybeTruncated, scala.Function1<String,Object> eventsFilter)`；Java 调用时传 `ReplayListenerBus.SELECT_ALL_FILTER()`。内部用 `JsonProtocol.sparkEventFromJson` 逐行还原事件。每行一个 JSON 事件。
- `org.apache.spark.scheduler.SparkListener`：抽象类，Java 直接 `extends` 并 override：
  `onJobStart/onJobEnd/onStageSubmitted/onStageCompleted/onTaskEnd/onEnvironmentUpdate/onExecutorAdded/onExecutorRemoved/onApplicationStart/onApplicationEnd/onOtherEvent(SparkListenerEvent)`。
- **SQL 事件**在 `org.apache.spark.sql.execution.ui` 包，**不在 SparkListener 的具名回调里**，统一从 `onOtherEvent` 接收并按类型分发：
  - `SparkListenerSQLExecutionStart`：字段 `executionId:Long`、`description:String`（**SQL 原文，含 `/* StatementID */`**）、`details:String`、`physicalPlanDescription:String`、`sparkPlanInfo:SparkPlanInfo`、`time:Long`。
  - `SparkListenerSQLExecutionEnd`：`executionId:Long`、`time:Long`。
  - `SparkListenerSQLAdaptiveExecutionUpdate`：AQE 计划更新（最终计划/有效分区）。
- **Thrift Server 事件**（可选，STS 才有）在 `org.apache.spark.sql.hive.thriftserver` 包：
  `SparkListenerThriftServerOperationStart`，字段含 `id:String`、`statement:String`（**SQL 原文，含 `/* StatementID */`**）、`sessionId`、`startTime`。**用类名字符串匹配处理，缺失不报错**（避免硬依赖 hive-thriftserver jar）。
- `JsonProtocol.sparkEventFromJson(String)` 在 `org.apache.spark.util` 包，`private[spark]`；优先**不直接调用**，让 replay 内部用它。仅在需要单行调试时反射调用并标 `// VERIFY@3.5.1`。
- Scala 互操作：Spark 3.5.1 使用 Scala 2.12，事件里的集合是 `scala.collection.Map/Seq`、可空值是 `scala.Option`。**一律在 `core` 内转成 Java 类型**（用 `scala.collection.JavaConverters`；不要用 Scala 2.13 的 `scala.jdk.javaapi`），上层模块只见 Java 类型。`TaskMetrics`/`TaskInfo`/`StageInfo` 的 getter 在 Java 中以方法形式访问。
- **JDK 9+ 反射封装**：Spark 3.5 在 JDK 9+ 上回放可能需放开模块封装，运行时加 `--add-opens=java.base/...`（见 `bin/sparkadvisor` 与设计文档 §10.1）。Java 8 不支持该参数，启动脚本会按 JVM 版本条件添加。
- **History Server 扩展点（M3）**：`org.apache.spark.status.AppHistoryServerPlugin` 是官方接口，SHS 通过 `ServiceLoader`（`META-INF/services/org.apache.spark.status.AppHistoryServerPlugin`）自动发现——**零侵入，jar 入 classpath 即可**，与 Spark SQL 自己的 tab 同机制。三个方法：`Seq<SparkListener> createListeners(SparkConf, ElementTrackingStore)`、`int displayOrder()`、`void setupUI(SparkUI)`。SQL tab 的 `SQLHistoryServerPlugin.setupUI` 即从 `ui.store` 建 `SQLTab().attachTab()`。
- **SparkAdvisor 采用"自给自足"集成（策略 B）**：`createListeners` 返回空（不干预 SHS 回放），`setupUI` 用我们自己的 `EventLogAnalyzer` 重解析建 tab。复用全部已验证栈、与 SHS store 解耦；日志懒解析（点开 tab 才解析）并按 app 缓存。
- **UI 内部类**（`SparkUI`/`SparkUITab`/`WebUIPage`/`UIUtils`）签名 VERIFY@3.5.1。`WebUIPage.render` 返回 `scala.xml.Seq[Node]`；SparkAdvisor tab 继承 `SparkUITab` 并固定到最后展示，页面通过 `SparkUiChrome` 调用 `UIUtils.headerSparkPage` 保留 Spark 标准页头和原生 tab 导航，报告主体用 `scala.xml.Unparsed` 嵌入并用 scoped CSS 适配 Spark 默认风格。

> 若上述任一签名在真实编译时不符，以 Spark 3.5.1 实际为准并就地修正，同时更新本节。

## 5. 模块与依赖方向

```
sparkadvisor-core      读取/解析/领域模型/StatementID 定位/指标聚合+关键路径(analyze 包)/finding+predict 契约  （依赖 spark-* provided）
sparkadvisor-analyzer  规则引擎 RuleEngine（产出 Finding）           → core            [M2 已实现]
sparkadvisor-predictor shuffle/executor 成本模型预测                 → core            [M2 已实现]
sparkadvisor-report    AnalysisResult 契约 + JSON + HTML 渲染        → core/analyzer/predictor [M2 已实现]
sparkadvisor-advisor   TuningAdvisor: RuleBasedAdvisor + LlmAdvisor（消费 AnalysisResult JSON，非 raw log）→ report [F4 已实现]
sparkadvisor-cli       picocli 入口，--advise none|rule|llm，产出报告               → core, report, advisor
sparkadvisor-ui-plugin History Server tab（AppHistoryServerPlugin/ServiceLoader）→ core/report, spark-* provided [M3 已实现]
```

> 注：设计文档把"指标聚合 + 关键路径"划在 analyzer。M1 实现时把这部分（`MetricAggregator`/`SqlAnalysis`/`StageAnalysis`）放在 **core 的 `analyze` 包**，这样 report 不必依赖尚未存在的 analyzer 模块就能渲染硬指标。analyzer 只负责**规则引擎**（产出 `Finding`），predictor 只负责**预测**；两者都消费 core 的 analyze 输出。`Finding`/`Recommendation`/`Severity` 在 `core/finding`，预测契约在 `core/predict`，使"产出方(analyzer/predictor)→core←消费方(report)"保持单向。

## 6. 代码风格

- 生产源码不可使用 Java 9+ 语法/API；不可变结果对象用普通 final 类（final 字段 + 构造器 + record 风格访问器）。
- 不使用 `record`、`sealed`、switch 表达式、text block、`var`、`Path.of`、`Files.writeString`、`CompletableFuture.orTimeout` 等 Java 9+ 能力；测试源码不受此限制。
- 包名根 `io.sparkadvisor`。公共 API 写 Javadoc；阈值类常量集中到 `analyzer` 的配置类，**不散落魔法数**。
- 解析层对“不完整/截断/缺失字段”必须容错：标注 `incomplete=true`，不抛异常中断整体分析。
- 日志用 `java.util.logging` 或 slf4j（若已在 classpath），不 `System.out` 打调试信息。

## 7. 测试

- 解析与定位必须有单测，输入用**最小化的 event log 片段文本**（直接放 `src/test/resources`），断言关键字段。
- StatementID 提取的必测用例：`/* abc_123 */ select ...`（命中）、无注释（置空）、注释在 SQL 中部（不误取）、`description` 与 `thriftserver.statement` 两路来源各一。
- 硬指标（倾斜比/利用率/偏离度）用构造的 task 分布做断言，数值与手算一致。
- 变更后至少运行 `mvn -q test`；涉及产物兼容性时运行 `mvn -q clean package` 并抽查 `target/classes` major version 为 52。

## 8. 当前进度与下一步

**M1 已完成**：父 POM、core/report/cli 模块、领域模型、分位数估计器、StatementID 提取/定位、`SparkEventCollector`、reader/parser、`MetricAggregator`、`AnalysisResult` 契约 + JSON/HTML、CLI `analyze` 端到端。

**M2 已完成**：
- `sparkadvisor-analyzer`：`RuleEngine` + 5 条规则（R1 倾斜、R2 spill、R3 并发不足、R6 GC、R8 调度），AQE 感知建议，阈值集中在 `RuleThresholds`。`Finding`/`Recommendation`/`Severity` 在 **core 的 `finding` 包**。
- `sparkadvisor-predictor`：`ShuffleCostModel`（§8.1 的 T(p) 模型）+ `ShufflePartitionPredictor`（扫描候选分区数，AQE 感知旋钮，倾斜短路为 SKEW_LIMITED）+ `ExecutorScalingPredictor`（贪心调度模拟，倾斜 max-task 作为加速下限，识别收益拐点）。预测结果契约 `ShufflePartitionPrediction`/`ExecutorScalingPrediction`/`Confidence` 在 **core 的 `predict` 包**，均带假设 + 置信度。
- `AnalysisResult` 新增 `shufflePrediction`/`executorPrediction` 字段；`AnalysisResultBuilder` 自动跑 analyzer + predictor；HTML 新增"Predictions"区（含假设/置信度/反转条件、executor 伸缩曲线表）。

**验证状态**：纯 Java 全栈（core+analyzer+predictor+report）已迁移为 Java 8 产物目标；analyzer 11 项 + predictor 5 项行为测试 + 端到端渲染（合成倾斜查询：4 条 finding、shuffle 预测正确判为 SKEW_LIMITED、executor 曲线被 9s straggler 正确封顶、AQE-on 不出现无用"开 AQE"建议）全部 PASS。

**M3 已完成（History Server tab）**：见 §4/§5；策略 B 自给自足，ServiceLoader 注册，部署见 DEPLOY.md。

**全部剩余任务已完成**：
- **core 增强**：`StageAnalysis` 加 input 字节/per-task input 中位数；新增 `ExecutorEvent` + `CoreTimeline`（从 ExecutorAdded/Removed 重建精确 core 时间线，积分 core-ms 算利用率，无事件时回退配置值）；`SqlAnalysis` 加 `physicalPlanText`；`SparkEventCollector` 抽 executor 事件。
- **规则补全**：R4 过并行小任务、R5 小文件（仅在有 input 字节的 scan stage 触发，shuffle stage 不误报）、R7 broadcast 机会（物理计划启发式：有 SMJ 且无 BroadcastJoin 才报，INFO + 明确 caveat）。RuleEngine 现 8 条规则。
- **F4 advisor 模块**：`TuningAdvisor` 接口 + `RuleBasedAdvisor`（确定性、离线、默认）+ `LlmAdvisor`（`LlmProvider` 接口 + `MinimaxLlmProvider` 默认 MiniMax-M2.5，`AnthropicLlmProvider` 可选 + `PromptBuilder` + `AdviceResponseParser`）。**核心原则落地**：喂模型的是结构化 `AnalysisResult`/`QueueAnalysisResult` JSON，绝非 raw log；`PromptBuilder`/`QueuePromptBuilder` 注释明确这点。LLM 失败优雅降级不抛异常。CLI 加 `--advise none|rule|llm`，`queue-report --advise none|llm`。`AnalysisResult.withAiAdvice()`/`QueueAnalysisResult.withAiAdvice()` 注入建议，HTML 渲染 provider+summary+建议。

**验证状态（JDK 21 编译 Java 8 产物 + 行为测试）**：CoreTimeline 积分（含动态分配/部分窗口）、R4/R5/R7（含负例）、RuleBasedAdvisor 端到端叙述、extractJson 去 fence/prose、LLM JSON 解析与降级、路径解析——全 PASS。`mvn -q clean package` 与 `mvn -q test` 已通过，生产 class major version 验证为 52；core eventlog 层、CLI、ui-plugin 触及 Spark 的类仍保留 `VERIFY@3.5.1`。

**项目已功能完整（M1–M3 + F4 + Q-M4）。** 后续可选优化：predictor 多点回归拟合 `o`/`r`；per-task 内存预算用 `spark.memory.fraction` 精算；本地 LLM provider。

## 9. 报告模块要点（M1 已实现，改动须遵守）

- **`AnalysisResult` 是唯一契约**，且**不得引入任何 Spark 类型**（保证可 JSON 序列化、report 模块与 core 的 provided Spark 依赖解耦）。core 的 `analyze` 包（`SqlAnalysis`/`StageAnalysis`/`MetricAggregator`）也是纯 Java。
- HTML 渲染是**单文件自包含**：内联 CSS、内联 SVG（关键路径三线条/队列趋势图）、底部内嵌完整 JSON。**不引前端构建链、不用 localStorage**。输出文件名包含 `_zh` 时生成中文报告，否则生成英文报告。
- 阈值常量 M1 暂放在 `HtmlReportWriter`（SKEW_WARN=5、GC_WARN=0.10、UTIL_LOW=0.40）；M2 RuleEngine 落地时把权威阈值集中到 analyzer 配置类，HTML 只读不再自带。

## 10. 常用命令

```bash
# 编译验证
mvn -q -DskipTests package

# 运行（集群节点，root 用户；脚本内含 source bigdata_env + kinit；JDK 9+ 才追加 --add-opens）
bin/sparkadvisor analyze \
  --path hdfs:///spark2x/eventLog/application_xxx \
  --statement-id 20260521_abc123 \
  --format html --out ./report.html
```
