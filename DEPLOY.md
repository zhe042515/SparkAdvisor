# SparkAdvisor 部署与使用指南

本文档说明 SparkAdvisor 的两种使用方式：

1. **Spark History Server UI 方式**：部署到 Spark History Server，作为 **SparkAdvisor** tab 使用。
2. **后台命令方式**：在集群客户端节点通过 `bin/sparkadvisor` 生成 HTML/JSON 报告。

两种方式都只读 HDFS event log，不向生产 Driver 注册 listener，不影响 Spark 作业正常执行。Spark/Hadoop 依赖均为 `provided`，运行时由集群 Spark/Hadoop classpath 提供。

## 1. 前置条件

- 构建使用 JDK 21；CLI 产物兼容 Java 8 运行时。History Server 插件运行在 SHS 当前 JVM 中。
- Spark 3.5.6 / Hadoop 运行时 classpath。
- 构建机器能访问 Maven Central。
- 集群侧能访问 event log 所在 HDFS 路径。
- Kerberos 环境按现有集群约定初始化：
  - CLI 由 `bin/sparkadvisor` 执行 `source /opt/client/bigdata_env` 与 `kinit`。
  - History Server 方式复用 SHS 进程已有 HDFS 凭据。

构建全量产物：

```bash
mvn -q -DskipTests package
```

核心产物：

```text
sparkadvisor-cli/target/sparkadvisor-cli.jar
sparkadvisor-ui-plugin/target/sparkadvisor-ui-plugin.jar
```

## 2. 方式一：Spark History Server UI 使用

SparkAdvisor 通过 Spark 官方扩展点 `org.apache.spark.status.AppHistoryServerPlugin` 接入 Spark History Server。插件由 Java `ServiceLoader` 自动发现，只要 jar 在 SHS classpath 中即可。

> 注意：这是 **History Server 插件**，不是运行中查询队列/driver 的 live Spark UI 插件。把 jar 加到查询队列、Spark Thrift Server 或应用 driver classpath 后，Environment 页可能能看到该 jar，但不会触发 `AppHistoryServerPlugin`，也不会在 live Spark UI 上新增 tab。必须让 **History Server 进程** 的 classpath 包含该 jar，并重启 History Server。

### 2.1 构建 UI 插件

```bash
mvn -q -DskipTests -pl sparkadvisor-ui-plugin -am package
```

产物：

```text
sparkadvisor-ui-plugin/target/sparkadvisor-ui-plugin.jar
```

该 jar 包含 SparkAdvisor 引擎、monitor 模块、Jackson、t-digest 等依赖；Spark/Hadoop 仍由 SHS 运行时提供，不打入 jar。

### 2.2 安装到 History Server classpath

推荐复制到 Spark 的 `jars/` 目录：

```bash
cp sparkadvisor-ui-plugin/target/sparkadvisor-ui-plugin.jar "$SPARK_HOME/jars/"
```

FusionInsight 环境示例：

```bash
export SPARK_HOME=/opt/client/Spark2x/spark
cp sparkadvisor-ui-plugin/target/sparkadvisor-ui-plugin.jar "$SPARK_HOME/jars/"
```

如果不希望改动 `jars/` 目录，也可以把插件加入 SHS classpath：

```bash
export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:/path/to/sparkadvisor-ui-plugin.jar"
```

### 2.3 配置 JDK 9+ module opens

Spark 3.5.6 在 JDK 9+ 上回放 event log 时可能会反射访问 `java.base` 内部包，SHS 进程需要增加以下 JVM 参数。Java 8 不支持也不需要这些参数。不要放到 `spark.history.ui.*` 配置里，应加到 SHS 启动 JVM 参数，例如 `SPARK_DAEMON_JAVA_OPTS`：

```bash
export SPARK_DAEMON_JAVA_OPTS="$SPARK_DAEMON_JAVA_OPTS \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
```

### 2.4 重启 History Server

```bash
"$SPARK_HOME/sbin/stop-history-server.sh"
"$SPARK_HOME/sbin/start-history-server.sh"
```

### 2.5 页面使用

打开 Spark History Server 中任意应用页面，导航栏会出现 **SparkAdvisor** tab。

- **队列模式**：`StatementID` 留空，展示该应用的队列级报告。适合长驻查询队列或运行中的 `.inprogress` 日志。大日志会在后台解析，页面提示稍后刷新；结果按 event log 快照大小/修改时间缓存。
- **单 SQL 下钻**：输入 SQL 开头注释中的 `StatementID`，例如 `/* 20260521_abc123 */`，点击 *Analyze SQL*，展示该语句的单 SQL 分析报告。

URL 形式：

```text
.../history/<appId>/sparkadvisor/
.../history/<appId>/sparkadvisor/?statementId=<ID>
```

### 2.6 UI 方式的工作机制

- 插件的 `createListeners` 返回空，不干预 SHS 自身 event log 回放。
- `setupUI` 挂载 SparkAdvisor tab；用户打开页面时，SparkAdvisor 使用自己的 `EventLogAnalyzer` 重新解析 event log。
- 队列模式使用 `sparkadvisor-monitor` 生成 `QueueAnalysisResult`，并启用轻量 `TaskInterval` 收集以分析资源争用。
- 队列分析在受限后台线程池中异步单飞执行，避免 10 GB 级 `.inprogress` 日志阻塞 SHS UI 请求线程。
- 队列页面会按 rolling event-log part 明细生成 snapshot key，并把 snapshot key + `top` + `samplePerStratum` + `bucket` 作为缓存/checkpoint key；默认目录为 `${java.io.tmpdir}/sparkadvisor-queue-checkpoints`，可用 `SPARKADVISOR_QUEUE_CHECKPOINT_DIR` 覆盖。当前 checkpoint 是结果 fast path，不是 byte-offset 增量回放；报告会以 `incremental=false` 和 `degradedReason` 保持诚实标注。
- 插件内部异常会被捕获并记录，不应影响 Spark History Server 其它页面。

### 2.7 Live Driver Spark UI 使用

如果需要在运行中的查询队列/Driver Spark UI 上直接显示 SparkAdvisor tab，需要使用 Spark 的 `spark.plugins` 机制。该方式不通过 `AppHistoryServerPlugin`，而是在 driver 内注册轻量 listener，从内存聚合快照渲染页面。

启动查询队列时确保 `sparkadvisor-ui-plugin.jar` 在 driver 启动 `SparkContext` 前可见，然后增加：

```bash
--conf spark.plugins=io.sparkadvisor.ui.live.SparkAdvisorSparkPlugin \
--conf spark.sparkadvisor.live.enabled=true
```

如果已有其它 SparkPlugin，使用逗号追加：

```bash
--conf spark.plugins=existing.Plugin,io.sparkadvisor.ui.live.SparkAdvisorSparkPlugin
```

可选参数：

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `spark.sparkadvisor.live.collectTaskIntervals` | `false` | 是否在 driver 内保留轻量 task interval，用于 live 队列页的资源占用/争用估计。长驻队列可能产生大量 task，默认关闭以控制 driver 内存。单 SQL 诊断不依赖该参数。 |
| `spark.sparkadvisor.live.retainedSqlExecutions` | `200` | Live 内存中最多保留的已完成 SQL；运行中的 SQL 不受此限制。淘汰 SQL 时同步释放其 Job 与 Stage。 |
| `spark.sparkadvisor.live.retainedJobs` | `2000` | 最多保留的已完成 Job，防止单条 SQL 产生异常多的轮次。 |
| `spark.sparkadvisor.live.retainedStages` | `5000` | 最多保留的已完成 Stage。Stage 完成后立即把 t-digest 固化为只读分布并释放构建器。 |
| `spark.sparkadvisor.live.retainedTaskIntervals` | `100000` | 开启 task interval 后的固定容量；超过后淘汰最早记录，不再随千万级 Task 线性增长。 |
| `spark.sparkadvisor.live.retainedExecutorEvents` | `4096` | Executor 增删事件容量；更早事件压缩成 core 基线。 |
| `spark.sparkadvisor.live.maxDescriptionChars` | `16384` | 单条 SQL description 最大字符数。 |
| `spark.sparkadvisor.live.maxPlanChars` | `65536` | 单条物理计划最大字符数。 |

上述限制只作用于 Live Driver。CLI 与 History Server 的离线 event log 回放仍保留完整数据。Live 页面发生淘汰后会声明当前报告来自保留窗口；全天完整报告应使用 event log 离线生成。

## 3. 方式二：后台命令使用

后台命令适合离线批处理、每日定时生成报告、或在不部署 History Server 插件时手工分析 event log。

推荐统一使用仓库内脚本：

```bash
bin/sparkadvisor <subcommand> [options]
```

脚本会执行：

- `source /opt/client/bigdata_env`
- `kinit -kt /opt/client/keytab/ossuser.keytab ossuser`
- 拼接集群 Spark/Hadoop classpath
- 默认添加 `-Xmx4g`，避免 Spark `JsonProtocol` 回放 100MB+ JSON event-log part 时因 JVM 默认堆过小 OOM；可用 `SPARKADVISOR_HEAP` 或 `SPARKADVISOR_JAVA_OPTS` 覆盖
- 在 JDK 9+ 上添加 Spark 3.5.6 需要的 `--add-opens`；Java 8 下不会添加

默认 jar 路径为：

```text
sparkadvisor-cli/target/sparkadvisor-cli.jar
```

如需指定其它 jar：

```bash
export SPARKADVISOR_JAR=/path/to/sparkadvisor-cli.jar
```

如需调整 CLI JVM 堆大小（默认 `4g`）：

```bash
export SPARKADVISOR_HEAP=8g
# 或直接传额外 JVM 参数；若其中包含 -Xmx，脚本不会再追加默认堆大小
export SPARKADVISOR_JAVA_OPTS="-Xmx8g -XX:+UseG1GC"
```

### 3.1 单 SQL 报告

按 StatementID 生成 HTML 报告：

```bash
bin/sparkadvisor analyze \
  --path hdfs:///spark2x/eventLog/application_1700000000000_0001 \
  --statement-id 20260521_abc123 \
  --advise rule \
  --format html \
  --out ./report.html
```

输出 JSON：

```bash
bin/sparkadvisor analyze \
  --path hdfs:///spark2x/eventLog/application_1700000000000_0001 \
  --statement-id 20260521_abc123 \
  --format json \
  --out ./analysis.json
```

不指定 `--statement-id` 时，CLI 会选择应用中最慢的 SQL 生成报告。

常用参数：

| 参数 | 说明 |
| --- | --- |
| `--path` | HDFS event log 路径，支持单文件或 rolling 目录。 |
| `--statement-id` | SQL 开头 `/* StatementID */` 中的 ID；纯数字可回退按 `executionId` 查找。 |
| `--format html|json` | 输出格式，默认 `html`。 |
| `--out` | 输出文件路径，默认 `report.<format>`。 |
| `--top` | 未指定 StatementID 时用于选择慢 SQL，当前报告输出最慢一条。 |
| `--keep-raw` | 调试用，保留原始 task 记录，会增加内存占用。 |
| `--hadoop-conf-dir` | 覆盖环境变量中的 Hadoop 配置目录。 |
| `--auth-to-local` | 覆盖 Hadoop `hadoop.security.auth_to_local` 规则；也可用环境变量 `SPARKADVISOR_AUTH_TO_LOCAL`。 |
| `--advise none|rule|llm` | Advisor 模式，默认 `rule`；`llm` 默认调用 MiniMax-M2.5，需要 `MINIMAX_API_KEY`。可用 `llm:claude` 走 Anthropic。 |
| `--lang auto|zh|en` | 报告语言，默认 `auto`；`auto` 下输出文件名包含 `_zh` 时生成中文，否则英文。 |

LLM 模式只发送结构化 `AnalysisResult` JSON，不发送 raw event log：

```bash
export MINIMAX_API_KEY=...
bin/sparkadvisor analyze \
  --path hdfs:///spark2x/eventLog/application_1700000000000_0001 \
  --statement-id 20260521_abc123 \
  --advise llm \
  --format html \
  --out ./report-llm_zh.html
```

报告语言可用 `--lang zh|en` 显式指定；默认 `auto` 保留“HTML 输出文件名包含 `_zh` 时生成中文报告”的兼容行为。
可选环境变量：`MINIMAX_MODEL` 覆盖默认模型，`MINIMAX_BASE_URL` 指向内部网关或代理。

### 3.2 队列级报告

分析一个长驻查询队列应用的一整轮 event log，生成队列级 HTML 报告：

```bash
bin/sparkadvisor queue-report \
  --path hdfs:///spark2x/eventLog/application_1700000000000_0001 \
  --format html \
  --out ./queue-report_zh.html \
  --top 50 \
  --bucket 1h \
  --advise llm
```

输出 JSON：

```bash
bin/sparkadvisor queue-report \
  --path hdfs:///spark2x/eventLog/application_1700000000000_0001 \
  --format json \
  --out ./queue-analysis.json \
  --top 50 \
  --bucket 1h
```

常用参数：

| 参数 | 说明 |
| --- | --- |
| `--path` | HDFS event log 路径，通常是一轮长驻队列应用的完整归档日志，也可读 `.inprogress` 快照。 |
| `--format html|json` | 输出格式，默认 `html`。 |
| `--out` | 输出文件路径，默认 `queue-report.<format>`。 |
| `--top` | 深度分析的最慢 SQL 数量，默认 50；其它 SQL 仍进入吞吐、延迟和趋势聚合。 |
| `--sample-per-stratum` | top-N 之外每类补充深度分析样本数，覆盖 spill/fetch/GC/skew/template 等分层，默认 5。 |
| `--bucket` | 时间分桶粒度，例如 `15m`、`1h`、`3600s`，默认 `1h`。 |
| `--hadoop-conf-dir` | 覆盖环境变量中的 Hadoop 配置目录。 |
| `--auth-to-local` | 覆盖 Hadoop `hadoop.security.auth_to_local` 规则；也可用环境变量 `SPARKADVISOR_AUTH_TO_LOCAL`。 |
| `--advise none|llm` | 队列级 AI Advisor，默认 `none`；`llm` 默认调用 MiniMax-M2.5，只发送结构化 `QueueAnalysisResult`。 |
| `--lang auto|zh|en` | 报告语言，默认 `auto`；`auto` 下输出文件名包含 `_zh` 时生成中文，否则英文。 |

队列报告包含：

- 查询吞吐与延迟 P50/P95/P99 趋势。
- 瓶颈聚类，例如倾斜、spill、小文件、GC 等反复出现的规则。
- 固定 executor/core 池的利用率时间序列。
- 争用受限查询、热点时段和资源大户。
- 队列级全局调参建议，带证据、置信度和预期覆盖范围。
- 可选 AI 队列建议（`--advise llm`），Provider 默认 MiniMax-M2.5。
- 内嵌完整 `QueueAnalysisResult` JSON。

## 4. 运行中日志与不完整数据

- `.inprogress` 日志可能缺少尾部事件，SparkAdvisor 会标注 `incomplete=true`。
- 运行中 SQL 不混入已完成 SQL 的延迟分位统计。
- rolling event log compaction 可能是有损的，报告会降低置信度。
- 队列争用是基于 task 占用率推断；event log 不直接记录排队等待。若启用 FAIR scheduler 或多个 pool，应降低争用归因置信度。

## 5. 常见问题

**找不到 SparkAdvisor tab**

- 确认访问的是 Spark History Server 的应用页面，URL 形如 `.../history/<appId>/...`；运行中 driver 的 live Spark UI 不会加载 `AppHistoryServerPlugin`。
- 确认 `sparkadvisor-ui-plugin.jar` 已在 **SHS 进程** classpath 中。Environment 页展示的是应用 event log 中记录的 driver/executor 环境，不代表 History Server 进程已加载该 jar。
- 确认 jar 内存在 `META-INF/services/org.apache.spark.status.AppHistoryServerPlugin`。
- 重启 History Server 后再打开应用页面。
- 检查 SHS 日志是否有 `SparkAdvisor tab attached to History Server UI`；若没有该日志，通常是 SHS classpath 未生效；若有 `Failed to attach SparkAdvisor tab`，按后面的异常栈排查。

**Live Driver Spark UI 找不到 SparkAdvisor tab**

- 确认查询队列启动参数包含 `spark.plugins=io.sparkadvisor.ui.live.SparkAdvisorSparkPlugin`，且未覆盖已有 plugin 列表。
- 确认 `spark.sparkadvisor.live.enabled=true`。
- 确认 `sparkadvisor-ui-plugin.jar` 在 driver 启动 `SparkContext` 前已进入 classpath；只在 Environment 页看到 jar 但未配置 `spark.plugins` 不会加载 live 插件。
- 检查 driver 日志是否有 `SparkAdvisor live tab attached to driver Spark UI` 或 `Failed to attach SparkAdvisor live tab`。

**解析时报 `InaccessibleObjectException`**

- JDK 9+ 运行时缺少 `--add-opens` 参数。CLI 使用 `bin/sparkadvisor`；SHS 方式检查 `SPARK_DAEMON_JAVA_OPTS`。Java 8 运行时不会使用该参数。

**HDFS 权限或 Kerberos 失败**

- CLI：确认脚本中的 `source /opt/client/bigdata_env` 与 `kinit` 可执行；如报 `No rules applied to user@REALM`，确认 `core-site.xml` 中的 `hadoop.security.auth_to_local` 可把 Kerberos principal 映射为本地短用户名，或临时使用 `--auth-to-local 'RULE:[1:$1@$0](.*@HADOOP.COM)s/@.*// DEFAULT'`。
- SHS：确认 History Server 本身能读取该 event log。

**队列页面长时间显示分析中**

- 大日志会后台解析。先刷新页面查看缓存结果。
- 检查 SHS 堆大小和日志；必要时增大 SHS heap。
- 建议长驻队列开启 rolling event log，控制单文件大小。
