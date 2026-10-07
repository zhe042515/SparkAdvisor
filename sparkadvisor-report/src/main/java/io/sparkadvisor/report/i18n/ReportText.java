package io.sparkadvisor.report.i18n;

import io.sparkadvisor.core.finding.Finding;
import io.sparkadvisor.core.finding.Recommendation;
import io.sparkadvisor.core.finding.Severity;
import io.sparkadvisor.core.predict.Confidence;
import io.sparkadvisor.core.util.Strings;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Small message catalog for user-facing report text.
 *
 * <p>AnalysisResult remains the stable JSON contract; this class only localizes report/advice
 * presentation text. Spark config keys, enum values, and rule IDs are intentionally preserved.
 */
public final class ReportText {

    private static final Map<String, String> ZH = new LinkedHashMap<String, String>();

    static {
        put("LIMIT required more than two probe rounds.", "LIMIT 取数超过两轮，可尝试增大初始探测分区数。");
        put("Try a larger first batch, capped by observed executor cores; compare the next execution.",
                "在已观测 Executor 总核数上限内增大首批分区数，并对比下一次执行。");
        put("Executor core count is unknown; verify the total before choosing a larger initial batch.",
                "Executor 总核数未知，请先确认核数上限，再选择更大的初始分区数。");
        put("The current value already reaches the observed executor core cap; no larger value is recommended.",
                "当前值已达到已观测 Executor 总核数上限，不再建议增大。");
        put("Two-round sizing uses observed partitions / (1 + max(2, scaleUpFactor)); it does not guarantee two rounds or a speedup.",
                "按实际累计探测分区数 / (1 + max(2, scaleUpFactor)) 估算两轮所需初始值；不保证两轮完成或一定加速。");
        put("scaleUpFactor is missing; use the observed second batch instead of assuming a default.",
                "日志缺少 scaleUpFactor，改用第二轮实际分区数作为候选，不假设默认值。");
        put("set spark.sql.adaptive.enabled=true; set spark.sql.adaptive.skewJoin.enabled=true",
                "设置 spark.sql.adaptive.enabled=true，并设置 spark.sql.adaptive.skewJoin.enabled=true");
        put("AQE is disabled; enabling adaptive skew-join handling lets Spark split skewed partitions automatically at runtime.",
                "当前 AQE 关闭；启用自适应倾斜 join 后，Spark 可以在运行时自动拆分倾斜分区。");
        put("Often the single most effective fix for join skew.",
                "对于 join 倾斜，这通常是成本最低且最有效的第一步。");

        put("set spark.sql.adaptive.skewJoin.enabled=true",
                "设置 spark.sql.adaptive.skewJoin.enabled=true");
        put("AQE is on but skew-join handling is off; turning it on lets Spark split skewed join partitions.",
                "AQE 已开启，但 skew join 处理未开启；打开后 Spark 可以拆分倾斜 join 分区。");
        put("Targets join skew specifically.", "专门针对 join 倾斜。");

        put("lower spark.sql.adaptive.skewJoin.skewedPartitionFactor and/or skewedPartitionThresholdInBytes",
                "调低 spark.sql.adaptive.skewJoin.skewedPartitionFactor 和/或 skewedPartitionThresholdInBytes");
        put("AQE skew-join is already enabled but skew remains; making the detector more aggressive can catch partitions it currently misses.",
                "AQE skew join 已开启但倾斜仍存在；把检测条件调得更敏感，可能捕获当前漏掉的倾斜分区。");
        put("Incremental; effectiveness depends on the actual key distribution.",
                "增量优化；实际效果取决于热点 key 的分布。");

        put("salt the skewed join/group key (add a random prefix, then aggregate in two passes)",
                "对倾斜的 join/group key 做 salting（加随机前缀分散，再二次聚合）");
        put("When a single key dominates, AQE's per-partition split has limited room; salting spreads that key across tasks.",
                "当单个 key 占比过高时，AQE 的分区拆分空间有限；salting 可以把热点 key 分散到多个 Task。");
        put("Most reliable for a known single hot key; requires query rewrite.",
                "已知单个热点 key 时最可靠，但需要改写 SQL。");

        put("lower spark.sql.adaptive.advisoryPartitionSizeInBytes",
                "调低 spark.sql.adaptive.advisoryPartitionSizeInBytes");
        put("Under AQE coalescing this advisory size controls partition size; a smaller value yields more, smaller partitions that fit in memory.",
                "AQE coalesce 开启时，该 advisory size 控制目标分区大小；调小后会产生更多、更小且更容易放入内存的分区。");
        put("Directly reduces per-task data and spill.", "直接降低单 Task 数据量和 spill 风险。");

        put("increase spark.sql.shuffle.partitions",
                "增大 spark.sql.shuffle.partitions");
        put("More partitions means less data per task, reducing the chance of spill.",
                "更多分区意味着每个 Task 处理的数据更少，从而降低 spill 概率。");
        put("Effective when the stage is not also skewed.", "当该 Stage 没有同时倾斜时更有效。");

        put("increase executor memory (spark.executor.memory / memoryOverhead)",
                "增大 executor memory（spark.executor.memory / memoryOverhead）");
        put("A larger per-task memory budget lets more of the partition stay in memory.",
                "更大的单 Task 内存预算可以让更多分区数据留在内存中。");
        put("Helps spill broadly but costs cluster memory.", "通常能缓解 spill，但会消耗更多集群内存。");

        put("lower spark.sql.adaptive.advisoryPartitionSizeInBytes (or raise coalescePartitions.initialPartitionNum)",
                "调低 spark.sql.adaptive.advisoryPartitionSizeInBytes（或调高 coalescePartitions.initialPartitionNum）");
        put("AQE may be coalescing into too few partitions; smaller advisory size keeps more partitions so more cores stay busy.",
                "AQE 可能把分区合并得过少；更小的 advisory size 可以保留更多分区，让更多 Core 有活可干。");
        put("Raises parallelism without a query rewrite.", "无需改写 SQL 即可提升并行度。");

        put("increase spark.sql.shuffle.partitions (or repartition the input)",
                "增大 spark.sql.shuffle.partitions（或对输入做 repartition）");
        put("More partitions spread work across more cores.", "更多分区可以把工作摊到更多 Core 上。");
        put("Effective when tasks are large and few.", "当 Task 数少且单 Task 较大时更有效。");

        put("raise spark.sql.adaptive.advisoryPartitionSizeInBytes",
                "调高 spark.sql.adaptive.advisoryPartitionSizeInBytes");
        put("Larger target partitions mean fewer, bigger tasks under AQE coalescing.",
                "AQE coalesce 下，更大的目标分区会生成更少、更大的 Task。");
        put("Reduces scheduling overhead.", "降低调度开销。");

        put("reduce spark.sql.shuffle.partitions, or coalesce() the result",
                "调低 spark.sql.shuffle.partitions，或对结果使用 coalesce()");
        put("Fewer partitions means fewer, larger tasks with less fixed overhead.",
                "更少分区意味着 Task 更少、更大，固定调度开销更低。");

        put("compact the source into larger files (e.g. periodic OPTIMIZE/compaction, or repartition on write)",
                "把源数据合并成更大的文件（例如周期性 OPTIMIZE/compaction，或写入时 repartition）");
        put("Fewer, larger files reduce task count and scheduling overhead on every read.",
                "文件更少且更大后，每次读取都会减少 Task 数和调度开销。");
        put("Addresses the root cause for all future reads.", "从根因上改善后续所有读取。");
        put("raise spark.sql.files.maxPartitionBytes / openCostInBytes to pack more small files per task",
                "调高 spark.sql.files.maxPartitionBytes / openCostInBytes，让每个 Task 合并读取更多小文件");
        put("Lets Spark combine more small files into each scan task.",
                "让 Spark 在每个 scan Task 中合并更多小文件。");
        put("Mitigates symptoms without rewriting data.", "不重写数据即可缓解症状。");

        put("increase executor memory or reduce per-task data (more partitions / smaller advisory size)",
                "增大 executor memory，或降低单 Task 数据量（更多分区 / 更小 advisory size）");
        put("Less live data per task lowers allocation churn and GC time.",
                "每个 Task 的活跃数据更少，可以降低对象分配压力和 GC 时间。");
        put("Pairs well with fixing any spill on the same stage.",
                "适合与同 Stage 的 spill 修复一起处理。");
        put("consider the G1 collector and review object-heavy UDFs",
                "考虑使用 G1 collector，并检查对象创建较重的 UDF");
        put("G1 handles large heaps better; heavy intermediate objects drive GC.",
                "G1 更适合大堆；大量中间对象通常会推高 GC。");
        put("Workload-dependent.", "取决于具体 workload。");

        put("raise spark.sql.autoBroadcastJoinThreshold if the small side fits in memory",
                "如果小表侧能放入内存，调高 spark.sql.autoBroadcastJoinThreshold");
        put("Lets Spark auto-broadcast a side under the threshold, replacing the shuffle join.",
                "让 Spark 自动广播低于阈值的一侧，从而替换 shuffle join。");
        put("Effective only when one side is genuinely small; too high risks driver OOM.",
                "仅当一侧确实足够小时有效；阈值过高会增加 driver OOM 风险。");
        put("add a broadcast() hint on the small side",
                "在小表侧添加 broadcast() hint");
        put("Forces a broadcast join regardless of the auto threshold when you know a side is small.",
                "当确认一侧很小时，可绕过自动阈值强制使用 broadcast join。");
        put("Explicit and reliable when the small side is known.", "小表侧明确时更直接可靠。");

        put("warm up executors (raise spark.dynamicAllocation.minExecutors) or pre-allocate resources",
                "预热 Executor（调高 spark.dynamicAllocation.minExecutors）或预分配资源");
        put("Cold-start executor acquisition delays the first tasks.",
                "Executor 冷启动会推迟首批 Task 启动。");
        put("Trades idle capacity for lower latency.", "用一定空闲容量换取更低延迟。");

        put("reduce shuffle volume before the exchange (pre-aggregate, filter earlier, or prune columns)",
                "在 Exchange 前减少 shuffle 数据量（提前聚合、提前过滤或裁剪列）");
        put("Lower shuffle volume reduces remote fetch pressure and reducer wait time.",
                "降低 shuffle 数据量可以减轻远端拉取压力和 reducer 等待时间。");
        put("Best when a large fraction of task time is fetch wait.",
                "当 Task 时间中 fetch wait 占比较高时收益最明显。");
        put("check shuffle service, network, and executor locality; increase reducer parallelism if reducers are too large",
                "检查 shuffle service、网络和 executor locality；如果 reducer 过大则提高 reducer 并行度");
        put("High fetch wait usually points to remote shuffle pressure rather than CPU work.",
                "较高 fetch wait 通常说明瓶颈在远端 shuffle 拉取，而不是 CPU 计算。");
        put("Operational fix; validate with network and shuffle-service metrics.",
                "偏运维侧修复；需要结合网络和 shuffle service 指标验证。");

        put("inspect failed task logs and executor/container health for this stage",
                "检查该 Stage 的失败 Task 日志以及 executor/container 健康状态");
        put("Retries add wall-clock noise and can hide the real bottleneck behind failed attempts.",
                "重试会增加墙钟耗时噪声，并可能掩盖真正的性能瓶颈。");
        put("Required before trusting fine-grained tuning recommendations.",
                "在信任细粒度调优建议前应先处理。");
        put("reduce per-task input/shuffle size if failures are memory or timeout related",
                "如果失败与内存或超时有关，降低单 Task 输入或 shuffle 数据量");
        put("Smaller tasks reduce memory pressure and timeout blast radius.",
                "更小的 Task 可以降低内存压力和超时影响范围。");
        put("Helps when failures correlate with large partitions.",
                "当失败与大分区相关时有效。");

        put("reduce sort/aggregate working set before the spilling operator",
                "在发生 spill 的 sort/aggregate 算子前减少工作集");
        put("The physical plan contains sort or aggregate operators and the SQL spills; reducing rows/columns before those operators lowers memory pressure.",
                "物理计划包含 sort 或 aggregate，且 SQL 发生 spill；在这些算子前减少行数/列数可以降低内存压力。");
        put("Usually more reliable than only increasing memory.",
                "通常比单纯加内存更可靠。");
        put("raise memory headroom or use smaller shuffle/advisory partitions for the spilling stage",
                "提高内存余量，或为发生 spill 的 Stage 使用更小的 shuffle/advisory 分区");
        put("Smaller operator inputs are less likely to spill during sort/aggregate.",
                "更小的算子输入更不容易在 sort/aggregate 阶段 spill。");
        put("Trade-off: more tasks and scheduling overhead.",
                "代价是 Task 数和调度开销可能上升。");

        put("Increase per-task memory headroom or reduce shuffle partition size",
                "增加单 Task 内存余量，或降低 shuffle 分区大小");
        put("A recurring spill pattern means many queue queries materialize partitions larger than available execution memory.",
                "反复出现 spill 说明队列内多条查询都会物化出超过可用执行内存的分区。");
        put("Expected to help queries represented by the repeated R2 findings.",
                "预计能改善反复命中 R2 的查询。");
        put("Audit AQE skew-join settings and skewed join keys",
                "审查 AQE skew join 配置和倾斜 join key");
        put("Repeated skew findings are usually key-distribution issues; changing only shuffle.partitions is unlikely to fix them.",
                "反复倾斜通常是 key 分布问题；只改 shuffle.partitions 大概率无法解决。");
        put("Expected to help skew-limited slow queries.",
                "预计能改善受倾斜限制的慢查询。");
        put("Use AQE coalescing/advisoryPartitionSizeInBytes instead of a single static shuffle.partitions value",
                "使用 AQE coalescing/advisoryPartitionSizeInBytes，避免依赖单一静态 shuffle.partitions");
        put("The queue contains both under-partitioned large queries and over-partitioned small queries, so one static partition count is fighting itself.",
                "队列里同时有分区不足的大查询和分区过多的小查询，单一静态分区数会互相牵制。");
        put("Expected to improve mixed workloads without penalizing small queries as much.",
                "预计能改善混合 workload，同时减少对小查询的副作用。");
        put("Increase fixed queue capacity or isolate/limit large resource-hog queries",
                "增加固定队列容量，或隔离/限制资源大户查询");
        put("The shared executor pool is saturated and many slow queries appear contention-limited.",
                "共享 executor 池已接近饱和，且多条慢查询表现为资源争用受限。");
        put("Expected to reduce latency for contention-limited queries.",
                "预计能降低争用受限查询的延迟。");
        put("Do not solve this queue by adding executors; prioritize SQL, layout, and plan fixes",
                "不要优先通过加 Executor 解决该队列；应先处理 SQL、数据布局和执行计划问题");
        put("The fixed pool is not consistently busy while queries still consume time.",
                "固定资源池并非持续繁忙，但查询仍然耗时较长。");
        put("Expected to prevent wasting executor resources on non-resource bottlenecks.",
                "避免把 Executor 浪费在非资源瓶颈上。");
        put("Compact upstream files and review spark.sql.files.maxPartitionBytes",
                "合并上游小文件，并检查 spark.sql.files.maxPartitionBytes");
        put("Repeated small-file scan findings indicate scheduling overhead and tiny input splits across the queue.",
                "反复小文件 scan 说明队列内普遍存在调度开销和过小输入 split。");
        put("Expected to improve scan-heavy queries.",
                "预计能改善 scan-heavy 查询。");
        put("Review executor memory, GC collector settings, serialization, and heavy UDF object churn",
                "检查 executor memory、GC collector、序列化方式，以及对象创建较重的 UDF");
        put("Repeated GC findings indicate JVM overhead is a queue-wide symptom, not an isolated query.",
                "反复 GC 命中说明 JVM 开销是队列级症状，而不是单条查询的孤立问题。");
        put("Expected to help GC-heavy slow queries.",
                "预计能改善 GC-heavy 慢查询。");

        put("Stage is skewed (max/median >= 5.0).",
                "该 Stage 存在倾斜（max/median >= 5.0）。");
        put("spark.sql.adaptive.advisoryPartitionSizeInBytes (shuffle.partitions is only the upper bound)",
                "spark.sql.adaptive.advisoryPartitionSizeInBytes（shuffle.partitions 只是上限）");
        put("Repartitioning rarely helps a skewed stage; address skew first (AQE skew-join / salting), then re-evaluate partition count.",
                "倾斜 Stage 通常不能靠重新分区解决；应先处理倾斜（AQE skew join / salting），再重新评估分区数。");
        put("Fixed-overhead share assumed at 20% of task time.",
                "假设固定开销占 Task 时间的 20%。");
        put("Throughput fit from a single operating point (median task).",
                "吞吐量基于单个运行点（中位数 Task）拟合。");
        put("If the stage is actually skewed or task time is dominated by fixed overhead, the optimum shifts; treat the recommended count as a starting point.",
                "如果该 Stage 实际存在倾斜，或 Task 时间主要由固定开销主导，最优点会偏移；建议分区数应作为起点而非保证。");
        put("Insufficient shuffle volume to model partition sizing.",
                "shuffle 数据量不足，无法可靠建模分区大小。");
        put("No actionable shuffle in this stage.", "该 Stage 没有可操作的 shuffle 调整点。");
        put("Each stage modeled as max(longestTask, waves * avgTask).",
                "每个 Stage 建模为 max(最长 Task, 批次数 * 平均 Task 时间)。");
        put("Skew caps the achievable speedup (longest task is irreducible).",
                "倾斜会限制可达加速比（最长 Task 无法被拆掉）。");
        put("Assumes adding cores does not change data layout or shuffle cost.",
                "假设增加 Core 不改变数据布局或 shuffle 成本。");
    }

    private ReportText() {}

    public static String t(ReportLanguage language, String en, String zh) {
        return language != null && language.isChinese() ? zh : en;
    }

    public static String severity(Severity severity, ReportLanguage language) {
        if (language == null || !language.isChinese()) {
            return severity.name();
        }
        switch (severity) {
            case CRITICAL:
                return "严重";
            case WARN:
                return "警告";
            case INFO:
            default:
                return "提示";
        }
    }

    public static String confidence(Confidence confidence, ReportLanguage language) {
        if (confidence == null || language == null || !language.isChinese()) {
            return confidence == null ? "" : confidence.name();
        }
        switch (confidence) {
            case HIGH:
                return "高";
            case MEDIUM:
                return "中";
            case LOW:
            default:
                return "低";
        }
    }

    public static String recommendationType(Recommendation.Type type, ReportLanguage language) {
        if (language == null || !language.isChinese()) {
            return type.name();
        }
        switch (type) {
            case SESSION_SET: return "会话参数";
            case RESTART_CONF: return "重启配置";
            case REWRITE:
            case SQL_REWRITE: return "SQL 改写";
            case GOVERNANCE: return "数据/平台治理";
            case SPARK_CONF:
            default: return "Spark 配置";
        }
    }

    public static String findingExplanation(Finding f, ReportLanguage language) {
        if (language == null || !language.isChinese()) {
            return f.explanation();
        }
        Integer stage = f.targetStageId();
        String sid = stage == null ? "SQL" : "Stage " + stage;
        if ("S-30".equals(f.ruleId())) {
            return "LIMIT 实际取数 " + evidence(f, "limit.rounds") + " 轮，累计探测 "
                    + evidence(f, "limit.scanned_partitions") + " 个分区；建议复核 spark.sql.limit.initialNumPartitions。";
        }
        if ("S-01".equals(f.ruleId())) {
            return sid + " 存在数据倾斜：最慢 Task 约为中位数的 "
                    + ratio(evidenceDouble(f, "task_duration.max_ms"), Math.max(1.0, evidenceDouble(f, "task_duration.p50_ms")))
                    + " 倍；该 Stage 受长尾 Task 限制，单纯增加 Executor 通常无效。";
        }
        if ("R1_DATA_SKEW".equals(f.ruleId())) {
            return sid + " 存在数据倾斜：最慢 Task 约为中位数的 "
                    + max(evidenceDouble(f, "durationSkewRatio"), evidenceDouble(f, "shuffleReadSkewRatio"))
                    + " 倍；该 Stage 受单个长尾 Task 限制，单纯增加 Executor 通常无效。";
        }
        if ("R2_EXCESSIVE_SPILL".equals(f.ruleId())) {
            return sid + " 的 spill 约为 shuffle read 数据量的 "
                    + pct(evidenceDouble(f, "spillRatio")) + "；分区相对单 Task 内存预算过大。";
        }
        if ("R3_LOW_PARALLELISM".equals(f.ruleId())) {
            return "Core 利用率只有 " + pct(evidenceDouble(f, "coreUtilization"))
                    + "；Executor slot 大量空闲，通常意味着分区过少或存在调度/资源等待。";
        }
        if ("R4_OVER_PARALLELISM".equals(f.ruleId())) {
            return sid + " 运行了 " + evidence(f, "numTasks") + " 个 Task，但中位耗时只有 "
                    + evidence(f, "medianTaskMs") + " ms；固定调度开销可能主导了有效计算。";
        }
        if ("R5_SMALL_FILES".equals(f.ruleId())) {
            return sid + " 读取源数据时产生 " + evidence(f, "numTasks") + " 个 Task，但每个 Task 中位输入只有约 "
                    + evidence(f, "medianInputBytesPerTask") + " bytes；这是典型小文件模式，Task 数由文件数量而非数据量驱动。";
        }
        if ("R6_GC_PRESSURE".equals(f.ruleId())) {
            return sid + " 的 Task 时间中约 " + pct(evidenceDouble(f, "gcRatio"))
                    + " 花在 GC 上，说明存在 JVM 内存压力。";
        }
        if ("R7_BROADCAST_JOIN".equals(f.ruleId())) {
            return "物理计划使用基于 shuffle 的 SortMergeJoin，且没有出现 broadcast join；如果某一侧足够小，broadcast 可以避免一次 shuffle。（启发式判断，需要确认小表侧实际大小。）";
        }
        if ("R8_SCHEDULING_DELAY".equals(f.ruleId())) {
            return sid + " 在首个 Task 启动前等待了约 " + pct(evidenceDouble(f, "delayRatio"))
                    + " 的 Stage 时间，说明存在资源获取或调度等待。";
        }
        if ("R9_SHUFFLE_FETCH_WAIT".equals(f.ruleId())) {
            return sid + " 的 shuffle fetch wait 占 Task 时间约 " + pct(evidenceDouble(f, "fetchWaitRatio"))
                    + "；瓶颈更像远端 shuffle 拉取、网络或 shuffle service 压力，而不是纯 CPU 计算。";
        }
        if ("R10_TASK_RETRY".equals(f.ruleId())) {
            return sid + " 出现 " + evidence(f, "failedTaskAttempts") + " 次失败 Task attempt，额外 attempt 数为 "
                    + evidence(f, "extraTaskAttempts") + "；重试会放大耗时并降低诊断置信度。";
        }
        if ("R11_SORT_AGG_SPILL".equals(f.ruleId())) {
            return "物理计划包含 sort/aggregate 类算子，且 SQL 总 spill 为 "
                    + evidence(f, "totalSpillBytes") + " bytes；高内存工作集很可能集中在排序或聚合阶段。";
        }
        return f.explanation();
    }

    public static Recommendation localize(Recommendation rec, ReportLanguage language) {
        if (language == null || !language.isChinese() || rec == null) {
            return rec;
        }
        return new Recommendation(rec.type(),
                localizeText(rec.action(), language),
                localizeText(rec.rationale(), language),
                localizeText(rec.expectedImpact(), language));
    }

    public static List<Recommendation> localizeRecommendations(List<Recommendation> recs,
                                                               ReportLanguage language) {
        if (recs == null) {
            return new ArrayList<Recommendation>();
        }
        List<Recommendation> out = new ArrayList<Recommendation>();
        for (Recommendation rec : recs) {
            out.add(localize(rec, language));
        }
        return out;
    }

    public static String localizeText(String value, ReportLanguage language) {
        if (Strings.isBlank(value) || language == null || !language.isChinese()) {
            return value;
        }
        String exact = ZH.get(value);
        if (exact != null) {
            return exact;
        }
        if (value.startsWith("Per-task memory budget = ")) {
            return value.replace("Per-task memory budget = ", "单 Task 内存预算 = ")
                    .replace(" bytes.", " bytes。");
        }
        if (value.startsWith("At least ") && value.contains(" completed queries show this evidence")) {
            return value.replace("At least ", "至少 ")
                    .replace(" of ", " / ")
                    .replace(" completed queries show this evidence in the analyzed slow-query set.",
                            " 个已完成查询在已分析慢查询集中出现该证据。");
        }
        if (value.contains(" affected ") && value.contains(" analyzed queries")) {
            return value.replace(" affected ", " 影响 ")
                    .replace(" analyzed queries", " 条已分析查询");
        }
        if (value.startsWith("No completed queries")) {
            return "该快照中没有已完成查询。";
        }
        if (value.startsWith("Covers both affected groups")) {
            return "覆盖已分析 Top 查询中的两类受影响查询。";
        }
        if (value.startsWith("Applies to the queue-level")) {
            return "适用于队列级容量决策。";
        }
        return value;
    }

    private static void put(String en, String zh) {
        ZH.put(en, zh);
    }

    private static String evidence(Finding f, String key) {
        if (f.evidence() == null) {
            return "?";
        }
        String value = f.evidence().get(key);
        return Strings.isBlank(value) ? "?" : value;
    }

    private static double evidenceDouble(Finding f, String key) {
        try {
            return Double.parseDouble(evidence(f, key));
        } catch (RuntimeException e) {
            return 0.0;
        }
    }

    private static double max(double a, double b) {
        return Math.max(a, b);
    }

    private static double ratio(double a, double b) {
        return b <= 0.0 ? 0.0 : a / b;
    }

    private static String pct(double v) {
        return String.format(java.util.Locale.ROOT, "%.0f%%", v * 100.0);
    }
}
