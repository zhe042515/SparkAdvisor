package io.sparkadvisor.core.eventlog;

import io.sparkadvisor.core.locate.StatementIdExtractor;
import io.sparkadvisor.core.metrics.MetricDistributionBuilder;
import io.sparkadvisor.core.model.ApplicationModel;
import io.sparkadvisor.core.model.Job;
import io.sparkadvisor.core.model.LimitProbe;
import io.sparkadvisor.core.model.SqlExecution;
import io.sparkadvisor.core.model.Stage;
import io.sparkadvisor.core.model.TaskInterval;
import io.sparkadvisor.core.model.TaskMetricStats;
import io.sparkadvisor.core.util.Java8Collections;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.SparkListenerExecutorAdded;
import org.apache.spark.scheduler.SparkListenerExecutorRemoved;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;

import io.sparkadvisor.core.model.ExecutorEvent;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * A custom {@link SparkListener} (written in Java) that ReplayListenerBus feeds events
 * into. It accumulates everything SparkAdvisor needs into Java-typed builders and then
 * materializes an immutable {@link ApplicationModel}.
 *
 * <h2>Design notes</h2>
 * <ul>
 *   <li><b>SQL events</b> (SparkListenerSQLExecutionStart/End, AQE updates) are NOT
 *       delivered through named callbacks; they arrive via {@link #onOtherEvent}. We keep
 *       a thin coupling to spark-sql types there. See {@code // VERIFY@3.5.1} markers.</li>
 *   <li><b>Thrift Server events</b> are matched by class name reflectively so we never
 *       hard-depend on hive-thriftserver being present.</li>
 *   <li><b>Memory</b>: per-stage metrics go straight into {@link MetricDistributionBuilder}.
 *       Live callers can supply finite retention limits; offline replay remains complete.</li>
 *   <li><b>Scala interop</b>: any Scala collections/Options returned by Spark are converted
 *       to Java types here so the rest of core/analyzer never sees Scala.</li>
 * </ul>
 *
 * <p>Not thread-safe; replay is single-threaded per bus.
 */
public final class SparkEventCollector extends SparkListener {

    private static final Logger LOG = Logger.getLogger(SparkEventCollector.class.getName());

    private static final String SQL_EXEC_START =
            "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart";
    private static final String SQL_EXEC_END =
            "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd";
    private static final String THRIFT_OP_START =
            "org.apache.spark.sql.hive.thriftserver.SparkListenerThriftServerOperationStart";

    private final StatementIdExtractor statementIdExtractor = new StatementIdExtractor();
    private final boolean collectTaskIntervals;
    private final EventRetentionPolicy retention;

    private String appId = "";
    private String appName = "";
    private long appStart = 0L;
    private long appEnd = 0L;
    private boolean incomplete = true; // set false once we see ApplicationEnd
    private final Map<String, String> conf = new LinkedHashMap<>();

    private final Map<Long, SqlExecBuilder> sqlExecs = new LinkedHashMap<>();
    private final Map<Integer, Job> jobs = new LinkedHashMap<>();
    private final Map<Integer, StageBuilder> activeStages = new LinkedHashMap<>();
    private final Map<Integer, Stage> completedStages = new LinkedHashMap<>();
    private final Map<Integer, Long> stageSqlExecutions = new HashMap<>();
    private final Deque<TaskInterval> taskIntervals = new ArrayDeque<>();
    private final Deque<String> pendingThriftStatementIds = new ArrayDeque<>();
    private final Deque<Long> completedSqlOrder = new ArrayDeque<>();
    private final Deque<Integer> completedJobOrder = new ArrayDeque<>();
    private final Deque<Integer> completedStageOrder = new ArrayDeque<>();

    private final Deque<ExecutorEvent> executorEvents = new ArrayDeque<>();
    private final Map<String, Integer> executorCores = new HashMap<>(); // executorId -> cores
    private long activeExecutorCores;
    private int compactedExecutorCores;
    private long compactedExecutorTime;
    private long evictedSqlExecutions;
    private long evictedJobs;
    private long evictedStages;
    private long evictedTaskIntervals;
    private long evictedExecutorEvents;
    private boolean truncatedSqlText;

    public SparkEventCollector() {
        this(false, EventRetentionPolicy.unbounded());
    }

    public SparkEventCollector(boolean collectTaskIntervals) {
        this(collectTaskIntervals, EventRetentionPolicy.unbounded());
    }

    public SparkEventCollector(boolean collectTaskIntervals, EventRetentionPolicy retention) {
        this.collectTaskIntervals = collectTaskIntervals;
        if (retention == null) throw new IllegalArgumentException("retention must not be null");
        this.retention = retention;
    }

    // ---- Application lifecycle -------------------------------------------------

    @Override
    public void onApplicationStart(SparkListenerApplicationStart e) {
        this.appName = e.appName();
        this.appStart = e.time();
        // appId is Option[String] in Scala; convert defensively.
        if (e.appId().isDefined()) {
            this.appId = e.appId().get();
        }
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd e) {
        this.appEnd = e.time();
        this.incomplete = false;
    }

    @Override
    public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate e) {
        // e.environmentDetails(): scala.collection.Map[String, Seq[(String,String)]]
        // We only need "Spark Properties". Convert via the interop helper.
        ScalaInterop.sparkProperties(e).forEach(conf::putIfAbsent);
    }

    @Override
    public void onExecutorAdded(SparkListenerExecutorAdded e) {
        // VERIFY@3.5.1: e.time():long, e.executorId():String, e.executorInfo().totalCores():int
        int cores = e.executorInfo().totalCores();
        Integer previous = executorCores.put(e.executorId(), cores);
        if (!"driver".equals(e.executorId())) {
            activeExecutorCores += cores - (previous == null ? 0L : previous.longValue());
        }
        addExecutorEvent(new ExecutorEvent(e.time(), cores, true));
    }

    @Override
    public void onExecutorRemoved(SparkListenerExecutorRemoved e) {
        // VERIFY@3.5.1: e.time():long, e.executorId():String
        Integer cores = executorCores.remove(e.executorId());
        if (cores != null) {
            if (!"driver".equals(e.executorId())) activeExecutorCores -= cores;
            addExecutorEvent(new ExecutorEvent(e.time(), cores, false));
        }
    }

    // ---- Jobs / Stages / Tasks -------------------------------------------------

    @Override
    public void onJobStart(SparkListenerJobStart e) {
        // sqlExecutionId is carried in job properties under "spark.sql.execution.id"
        Long sqlId = ScalaInterop.sqlExecutionId(e.properties());
        // stageIds: Scala Seq[Object]; convert to Java List<Integer>
        List<Integer> stageIds = ScalaInterop.intSeq(e.stageIds());
        jobs.put(e.jobId(), new Job(e.jobId(), sqlId, stageIds, e.time(), 0L, false, limitProbe(e, sqlId)));
        if (sqlId != null) {
            sqlExecs.computeIfAbsent(sqlId, SqlExecBuilder::new).jobIds.add((long) e.jobId());
            SqlExecBuilder sql = sqlExecs.get(sqlId);
            for (Integer stageId : stageIds) {
                Long existing = stageSqlExecutions.putIfAbsent(stageId, sqlId);
                if (existing == null || existing.equals(sqlId)) sql.stageIds.add(stageId);
            }
        }
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd e) {
        Job j = jobs.get(e.jobId());
        if (j != null && j.completionTime() == 0L) {
            boolean failed = !e.jobResult().getClass().getName().contains("JobSucceeded");
            jobs.put(e.jobId(), new Job(j.jobId(), j.sqlExecutionId(), j.stageIds(),
                    j.submissionTime(), e.time(), failed, j.limitProbe()));
            completedJobOrder.addLast(e.jobId());
            evictCompletedJobs();
        }
    }

    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted e) {
        StageInfo info = e.stageInfo();
        completedStages.remove(info.stageId());
        removeAll(completedStageOrder, Integer.valueOf(info.stageId()));
        StageBuilder b = activeStages.get(info.stageId());
        if (b == null || b.attemptId != info.attemptNumber()) {
            b = new StageBuilder();
            activeStages.put(info.stageId(), b);
        }
        b.stageId = info.stageId();
        b.attemptId = info.attemptNumber();              // VERIFY@3.5.1 (attemptNumber vs attemptId)
        b.numTasks = info.numTasks();
        b.parentStageIds = ScalaInterop.intSeq(info.parentIds());
        // submissionTime is Option[Long]
        b.submissionTime = ScalaInterop.optLong(info.submissionTime());
    }

    private LimitProbe limitProbe(SparkListenerJobStart event, Long sqlId) {
        // VERIFY@3.5.6: one ResultStage per simple executeTake job; rddInfos.head is its RDD.
        // Store scalars only. No extra TaskEnd work and no full properties/RDD object retention.
        SqlExecBuilder sql = sqlId == null ? null : sqlExecs.get(sqlId);
        if (sql == null || !sql.physicalPlanText.contains("CollectLimit") || event.stageInfos().size() != 1) return null;
        StageInfo stage = event.stageInfos().apply(0);
        if (stage.attemptNumber() != 0 || !stage.parentIds().isEmpty() || stage.rddInfos().isEmpty()) return null;
        org.apache.spark.storage.RDDInfo rdd = stage.rddInfos().apply(0);
        return new LimitProbe(rdd.id(), rdd.numPartitions(), stage.numTasks(),
                limitConfig(event.properties(), LimitProbe.INITIAL_PARTITIONS),
                limitConfig(event.properties(), LimitProbe.SCALE_UP_FACTOR),
                (int) Math.min(Integer.MAX_VALUE, Math.max(0L, activeExecutorCores)));
    }

    private static int limitConfig(java.util.Properties properties, String key) {
        String value = properties == null ? null : properties.getProperty(key);
        if (value == null) return 0;
        try {
            int parsed = Integer.parseInt(value.trim());
            return parsed > 0 ? parsed : -1;
        } catch (NumberFormatException invalid) {
            return -1;
        }
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted e) {
        StageInfo info = e.stageInfo();
        StageBuilder b = activeStages.computeIfAbsent(info.stageId(), id -> new StageBuilder());
        b.stageId = info.stageId();
        b.numTasks = info.numTasks();
        b.completionTime = ScalaInterop.optLong(info.completionTime());
        completedStages.put(info.stageId(), b.toStage());
        activeStages.remove(info.stageId());
        removeAll(completedStageOrder, Integer.valueOf(info.stageId()));
        completedStageOrder.addLast(info.stageId());
        evictCompletedStages();
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd e) {
        StageBuilder b = activeStages.computeIfAbsent(e.stageId(), id -> new StageBuilder());
        b.taskEndCount++;
        String reason = e.reason() == null ? "" : e.reason().getClass().getName();
        boolean failedAttempt = !reason.contains("Success");
        if (failedAttempt) {
            b.failedTaskEndCount++;
        }
        // Track earliest task launch for scheduling-delay computation.
        long launch = e.taskInfo().launchTime();
        if (b.firstTaskLaunch == 0L || launch < b.firstTaskLaunch) {
            b.firstTaskLaunch = launch;
        }
        TaskMetrics m = e.taskMetrics();
        if (collectTaskIntervals && retention.taskIntervals() > 0) {
            long finish = e.taskInfo().finishTime(); // VERIFY@3.5.1
            if (finish > 0L && launch > 0L && finish >= launch) {
                long executorRunTimeMs = m == null ? 0L : m.executorRunTime(); // VERIFY@3.5.1
                long executorCpuTimeNs = m == null ? 0L : m.executorCpuTime(); // VERIFY@3.5.1
                long jvmGcTimeMs = m == null ? 0L : m.jvmGCTime(); // VERIFY@3.5.1
                long fetchWaitMs = m == null ? 0L : m.shuffleReadMetrics().fetchWaitTime(); // VERIFY@3.5.1
                taskIntervals.addLast(new TaskInterval(
                        e.taskInfo().taskId(),
                        e.stageId(),
                        e.stageAttemptId(),
                        stageSqlExecutions.get(e.stageId()),
                        retention.retainTaskExecutorId() ? e.taskInfo().executorId() : "",
                        launch,
                        finish,
                        executorRunTimeMs,
                        executorCpuTimeNs,
                        jvmGcTimeMs,
                        fetchWaitMs,
                        failedAttempt,
                        e.taskInfo().speculative())); // VERIFY@3.5.1
                evictTaskIntervals();
            }
        }
        if (m == null) {
            return; // failed/speculative task without metrics
        }
        // Pull raw metrics; field accessors verified against Spark 3.5.1 TaskMetrics.
        long executorRunTime = m.executorRunTime();                      // VERIFY@3.5.1
        if (executorRunTime > b.maxTaskDurationMs) {
            b.maxTaskDurationMs = executorRunTime;
            b.maxTaskId = e.taskInfo().taskId();                         // VERIFY@3.5.1
        }
        b.duration.add(executorRunTime);
        b.gc.add(m.jvmGCTime());                                   // VERIFY@3.5.1
        b.deserialize.add(m.executorDeserializeTime());            // VERIFY@3.5.1
        b.memorySpill.add(m.memoryBytesSpilled());
        b.diskSpill.add(m.diskBytesSpilled());
        b.input.add(m.inputMetrics().bytesRead());
        b.output.add(m.outputMetrics().bytesWritten());
        b.shuffleRead.add(m.shuffleReadMetrics().totalBytesRead()); // VERIFY@3.5.1
        b.shuffleWrite.add(m.shuffleWriteMetrics().bytesWritten()); // VERIFY@3.5.1
        b.shuffleFetchWaitMs += m.shuffleReadMetrics().fetchWaitTime(); // VERIFY@3.5.1
        b.shuffleRemoteReadBytes += m.shuffleReadMetrics().remoteBytesRead(); // VERIFY@3.5.1
    }

    // ---- SQL + Thrift events (arrive via onOtherEvent) -------------------------

    @Override
    public void onOtherEvent(SparkListenerEvent event) {
        String cls = event.getClass().getName();
        switch (cls) {
            case SQL_EXEC_START:
                handleSqlStart(event);
                break;
            case SQL_EXEC_END:
                handleSqlEnd(event);
                break;
            case THRIFT_OP_START:
                handleThriftOpStart(event);
                break;
            default:
                break;
        }
    }

    private void handleSqlStart(SparkListenerEvent event) {
        // Accessed via the spark-sql type. VERIFY@3.5.1 field names:
        // executionId:Long, description:String, physicalPlanDescription:String, time:Long
        SparkListenerSQLExecutionStart s = SqlEventAccess.sqlExecutionStart(event);
        SqlExecBuilder b = sqlExecs.computeIfAbsent(s.executionId(), SqlExecBuilder::new);
        b.description = truncate(s.description(), retention.descriptionChars());
        b.physicalPlanText = truncate(s.physicalPlanDescription(), retention.physicalPlanChars());
        truncatedSqlText |= length(s.description()) > retention.descriptionChars()
                || length(s.physicalPlanDescription()) > retention.physicalPlanChars();
        b.startTime = s.time();
        statementIdExtractor.extract(s.description()).ifPresent(id -> b.statementId = id);
        if (b.statementId == null && !pendingThriftStatementIds.isEmpty()) {
            b.statementId = pendingThriftStatementIds.removeFirst();
        }
    }

    private void handleSqlEnd(SparkListenerEvent event) {
        SparkListenerSQLExecutionEnd s = SqlEventAccess.sqlExecutionEnd(event);
        SqlExecBuilder b = sqlExecs.computeIfAbsent(s.executionId(), SqlExecBuilder::new);
        boolean firstEnd = b.endTime == 0L;
        b.endTime = s.time();
        // VERIFY@3.5.6: nonempty errorMessage denotes a failed SQL, even if earlier jobs succeeded.
        b.failed = s.errorMessage().isDefined() && !s.errorMessage().get().isEmpty();
        if (firstEnd) {
            completedSqlOrder.addLast(s.executionId());
            evictCompletedSqlExecutions();
        }
    }

    private void handleThriftOpStart(SparkListenerEvent event) {
        // Supplementary StatementID source. Reflective access keeps hive-thriftserver optional.
        SqlEventAccess.thriftStatement(event).ifPresent(stmt ->
                statementIdExtractor.extract(stmt).ifPresent(id ->
                        // Attach to the most recent SQL exec lacking a statementId, if any.
                        attachThriftStatementId(id)));
    }

    private void attachThriftStatementId(String id) {
        // Best-effort: STS operation start typically precedes the SQL execution. Keep the
        // id and attach it to the next SQLExecutionStart whose description lacks a leading
        // StatementID; if the SQL event already exists, fill the first one still missing an id.
        for (SqlExecBuilder b : sqlExecs.values()) {
            if (b.statementId == null) {
                b.statementId = id;
                return;
            }
        }
        pendingThriftStatementIds.addLast(id);
        while (pendingThriftStatementIds.size() > retention.pendingStatementIds()) {
            pendingThriftStatementIds.removeFirst();
        }
    }

    // ---- Materialization -------------------------------------------------------

    public ApplicationModel build() {
        List<SqlExecution> execList = new ArrayList<>();
        for (SqlExecBuilder b : sqlExecs.values()) {
            boolean execIncomplete = b.startTime == 0L || b.endTime == 0L;
            execList.add(new SqlExecution(
                    b.executionId, b.statementId, b.description, b.physicalPlanText,
                    b.startTime, b.endTime, execIncomplete, Java8Collections.listCopy(b.jobIds), b.failed));
        }
        List<Stage> stageList = new ArrayList<>(completedStages.values());
        for (StageBuilder b : activeStages.values()) {
            stageList.add(b.toStage());
        }
        List<ExecutorEvent> executorEventList = new ArrayList<>();
        if (compactedExecutorCores > 0) {
            executorEventList.add(new ExecutorEvent(compactedExecutorTime, compactedExecutorCores, true));
        }
        executorEventList.addAll(executorEvents);
        return new ApplicationModel(
                appId, appName, appStart, appEnd, incomplete,
                Java8Collections.mapCopy(conf), Java8Collections.listCopy(execList),
                Java8Collections.listCopy(jobs.values()), Java8Collections.listCopy(stageList),
                Java8Collections.listCopy(executorEventList), Java8Collections.listCopy(taskIntervals));
    }

    /** Describes whether the live model has discarded old detail to stay within its budget. */
    public String retentionSummary() {
        if (evictedSqlExecutions == 0L && evictedJobs == 0L && evictedStages == 0L
                && evictedTaskIntervals == 0L && evictedExecutorEvents == 0L
                && !truncatedSqlText) return "";
        return "Live retention window discarded old detail: sql=" + evictedSqlExecutions
                + ", jobs=" + evictedJobs + ", stages=" + evictedStages
                + ", taskIntervals=" + evictedTaskIntervals
                + ", executorEvents=" + evictedExecutorEvents
                + ", sqlTextTruncated=" + truncatedSqlText + ".";
    }

    private void evictCompletedSqlExecutions() {
        while (completedSqlOrder.size() > retention.completedSqlExecutions()) {
            Long executionId = completedSqlOrder.removeFirst();
            SqlExecBuilder removed = sqlExecs.remove(executionId);
            if (removed == null) continue;
            evictedSqlExecutions++;
            for (Long jobId : removed.jobIds) {
                Job job = jobs.remove(jobId.intValue());
                if (job != null) evictedJobs++;
            }
            removeStagesForExecution(removed);
            logEviction("SQL executions", evictedSqlExecutions);
        }
    }

    private void removeStagesForExecution(SqlExecBuilder sql) {
        for (Integer stageId : sql.stageIds) {
            if (!Long.valueOf(sql.executionId).equals(stageSqlExecutions.get(stageId))) continue;
            stageSqlExecutions.remove(stageId);
            activeStages.remove(stageId);
            if (completedStages.remove(stageId) != null) evictedStages++;
            removeAll(completedStageOrder, stageId);
        }
    }

    private void evictCompletedJobs() {
        while (completedJobOrder.size() > retention.completedJobs()) {
            Integer jobId = completedJobOrder.removeFirst();
            Job removed = jobs.remove(jobId);
            if (removed != null) {
                if (removed.sqlExecutionId() != null) {
                    SqlExecBuilder sql = sqlExecs.get(removed.sqlExecutionId());
                    if (sql != null) sql.jobIds.remove(Long.valueOf(jobId.longValue()));
                }
                evictedJobs++;
                logEviction("jobs", evictedJobs);
            }
        }
    }

    private void evictCompletedStages() {
        while (completedStageOrder.size() > retention.completedStages()) {
            Integer stageId = completedStageOrder.removeFirst();
            if (completedStages.remove(stageId) != null) {
                Long sqlId = stageSqlExecutions.remove(stageId);
                if (sqlId != null) {
                    SqlExecBuilder sql = sqlExecs.get(sqlId);
                    if (sql != null) sql.stageIds.remove(stageId);
                }
                evictedStages++;
                logEviction("stages", evictedStages);
            }
        }
    }

    private void evictTaskIntervals() {
        while (taskIntervals.size() > retention.taskIntervals()) {
            taskIntervals.removeFirst();
            evictedTaskIntervals++;
            logEviction("task intervals", evictedTaskIntervals);
        }
    }

    private void addExecutorEvent(ExecutorEvent event) {
        executorEvents.addLast(event);
        while (executorEvents.size() > retention.executorEvents()) {
            ExecutorEvent old = executorEvents.removeFirst();
            evictedExecutorEvents++;
            compactedExecutorCores += old.added() ? old.cores() : -old.cores();
            if (compactedExecutorCores < 0) compactedExecutorCores = 0;
            compactedExecutorTime = executorEvents.isEmpty() ? old.timeMs() : executorEvents.peekFirst().timeMs();
        }
    }

    private static String truncate(String value, int maxChars) {
        if (value == null || maxChars == 0) return "";
        return value.length() <= maxChars ? value : value.substring(0, maxChars);
    }

    private static int length(String value) {
        return value == null ? 0 : value.length();
    }

    private static <T> void removeAll(Deque<T> deque, T value) {
        while (deque.removeFirstOccurrence(value)) {
            // Stage retries can otherwise leave an old queue entry that evicts the new attempt.
        }
    }

    private static void logEviction(String type, long count) {
        if (count == 1L || (count & (count - 1L)) == 0L) {
            LOG.info("SparkAdvisor live retention discarded " + count + " old " + type);
        }
    }

    // ---- Mutable builders ------------------------------------------------------

    private static final class SqlExecBuilder {
        final long executionId;
        String statementId;
        String description = "";
        String physicalPlanText = "";
        long startTime = 0L;
        long endTime = 0L;
        boolean failed;
        final Set<Long> jobIds = new LinkedHashSet<>();
        final Set<Integer> stageIds = new HashSet<>();

        SqlExecBuilder(long executionId) {
            this.executionId = executionId;
        }
    }

    private static final class StageBuilder {
        int stageId;
        int attemptId;
        int numTasks;
        List<Integer> parentStageIds = Java8Collections.listOf();
        long submissionTime = 0L;
        long firstTaskLaunch = 0L;
        long completionTime = 0L;
        int taskEndCount;
        int failedTaskEndCount;
        long shuffleFetchWaitMs;
        long shuffleRemoteReadBytes;
        long maxTaskId = -1L;
        long maxTaskDurationMs = -1L;

        final MetricDistributionBuilder duration = new MetricDistributionBuilder();
        final MetricDistributionBuilder shuffleRead = new MetricDistributionBuilder();
        final MetricDistributionBuilder shuffleWrite = new MetricDistributionBuilder();
        final MetricDistributionBuilder input = new MetricDistributionBuilder();
        final MetricDistributionBuilder output = new MetricDistributionBuilder();
        final MetricDistributionBuilder memorySpill = new MetricDistributionBuilder();
        final MetricDistributionBuilder diskSpill = new MetricDistributionBuilder();
        final MetricDistributionBuilder gc = new MetricDistributionBuilder();
        final MetricDistributionBuilder deserialize = new MetricDistributionBuilder();

        Stage toStage() {
            TaskMetricStats stats = new TaskMetricStats(
                    duration.build(), shuffleRead.build(), shuffleWrite.build(),
                    input.build(), output.build(), memorySpill.build(),
                    diskSpill.build(), gc.build(), deserialize.build());
            return new Stage(
                    stageId, attemptId, numTasks, parentStageIds,
                    submissionTime, firstTaskLaunch, completionTime,
                    shuffleRead.build().sum(), shuffleWrite.build().sum(),
                    shuffleFetchWaitMs, shuffleRemoteReadBytes,
                    failedTaskEndCount, Math.max(0, taskEndCount - numTasks), maxTaskId, stats);
        }
    }
}
