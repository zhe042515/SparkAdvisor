package io.sparkadvisor.core.eventlog;

/**
 * Memory bounds used by a live {@link SparkEventCollector}.
 *
 * <p>Offline event-log replay uses {@link #unbounded()} so reports preserve the complete log.
 * Live Driver integrations should provide finite values because their collector can live for
 * the entire application lifetime.
 */
public final class EventRetentionPolicy {

    private static final int UNBOUNDED = Integer.MAX_VALUE;
    private static final int LIVE_SQL_EXECUTIONS = 200;
    private static final int LIVE_JOBS = 2_000;
    private static final int LIVE_STAGES = 5_000;
    private static final int LIVE_TASK_INTERVALS = 100_000;
    private static final int LIVE_EXECUTOR_EVENTS = 4_096;
    private static final int LIVE_PENDING_STATEMENT_IDS = 256;
    private static final int LIVE_DESCRIPTION_CHARS = 16_384;
    private static final int LIVE_PHYSICAL_PLAN_CHARS = 65_536;

    private final int completedSqlExecutions;
    private final int completedJobs;
    private final int completedStages;
    private final int taskIntervals;
    private final int executorEvents;
    private final int pendingStatementIds;
    private final int descriptionChars;
    private final int physicalPlanChars;
    private final boolean retainTaskExecutorId;

    public EventRetentionPolicy(int completedSqlExecutions, int completedJobs,
                                int completedStages, int taskIntervals,
                                int executorEvents, int pendingStatementIds,
                                int descriptionChars, int physicalPlanChars,
                                boolean retainTaskExecutorId) {
        this.completedSqlExecutions = nonNegative(completedSqlExecutions);
        this.completedJobs = nonNegative(completedJobs);
        this.completedStages = nonNegative(completedStages);
        this.taskIntervals = nonNegative(taskIntervals);
        this.executorEvents = nonNegative(executorEvents);
        this.pendingStatementIds = nonNegative(pendingStatementIds);
        this.descriptionChars = nonNegative(descriptionChars);
        this.physicalPlanChars = nonNegative(physicalPlanChars);
        this.retainTaskExecutorId = retainTaskExecutorId;
    }

    public static EventRetentionPolicy unbounded() {
        return new EventRetentionPolicy(UNBOUNDED, UNBOUNDED, UNBOUNDED, UNBOUNDED,
                UNBOUNDED, UNBOUNDED, UNBOUNDED, UNBOUNDED, true);
    }

    /** Defaults sized for a long-running Driver with high task volume. */
    public static EventRetentionPolicy liveDefaults() {
        return new EventRetentionPolicy(LIVE_SQL_EXECUTIONS, LIVE_JOBS, LIVE_STAGES,
                LIVE_TASK_INTERVALS, LIVE_EXECUTOR_EVENTS, LIVE_PENDING_STATEMENT_IDS,
                LIVE_DESCRIPTION_CHARS, LIVE_PHYSICAL_PLAN_CHARS, false);
    }

    public int completedSqlExecutions() { return completedSqlExecutions; }
    public int completedJobs() { return completedJobs; }
    public int completedStages() { return completedStages; }
    public int taskIntervals() { return taskIntervals; }
    public int executorEvents() { return executorEvents; }
    public int pendingStatementIds() { return pendingStatementIds; }
    public int descriptionChars() { return descriptionChars; }
    public int physicalPlanChars() { return physicalPlanChars; }
    public boolean retainTaskExecutorId() { return retainTaskExecutorId; }

    private static int nonNegative(int value) {
        if (value < 0) {
            throw new IllegalArgumentException("retention limit must be >= 0: " + value);
        }
        return value;
    }
}
