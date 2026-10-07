package io.sparkadvisor.core.analyze;

import io.sparkadvisor.core.model.ApplicationModel;
import io.sparkadvisor.core.model.Job;
import io.sparkadvisor.core.model.SqlExecution;
import io.sparkadvisor.core.model.Stage;
import io.sparkadvisor.core.model.TaskInterval;
import io.sparkadvisor.core.util.Java8Collections;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Turns the raw {@link ApplicationModel} into a {@link SqlAnalysis} for a target SQL.
 *
 * <p>Links SQL -&gt; jobs -&gt; stages, then computes:
 * <ul>
 *   <li>per-stage hard metrics ({@link StageAnalysis})</li>
 *   <li>critical path over the stage DAG (longest path weighted by per-stage max-task time)</li>
 *   <li>ideal time (per-stage totalTaskTime / availableCores along the same path)</li>
 *   <li>core utilization across the SQL's stages</li>
 * </ul>
 *
 * <p>M1 simplification: {@code availableCores} is read from configuration
 * ({@code spark.executor.instances} * {@code spark.executor.cores}) when present, else
 * defaults to 1 so utilization is still defined. A precise core timeline from
 * ExecutorAdded/Removed events is an M2 refinement (design §7.2 / §8.3).
 */
public final class MetricAggregator {

    private final ApplicationModel app;
    private final Map<Integer, Stage> stagesById;
    private final int availableCores;
    private final CoreTimeline coreTimeline;

    public MetricAggregator(ApplicationModel app) {
        this.app = app;
        this.stagesById = new HashMap<>();
        for (Stage s : app.stages()) {
            // Keep the latest attempt seen for a given stageId.
            stagesById.put(s.stageId(), s);
        }
        this.availableCores = readAvailableCores(app.conf());
        // Accurate capacity from executor add/remove events; falls back to config-derived cores
        // when the events weren't captured (e.g. logStageExecutorMetrics disabled).
        this.coreTimeline = CoreTimeline.from(app.executorEvents(), availableCores);
    }

    public SqlAnalysis analyze(SqlExecution sql) {
        List<Stage> stages = stagesFor(sql);
        List<StageAnalysis> stageAnalyses = Java8Collections.listCopy(
                stages.stream().map(StageAnalysis::from)
                        .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new)));

        long criticalPath = criticalPath(stages);
        long ideal = idealTime(stages);
        long wall = sql.wallClockMs();
        double deviation = criticalPath == 0 ? 0.0
                : (double) (wall - criticalPath) / (double) criticalPath;

        long totalTaskTime = stageAnalyses.stream().mapToLong(StageAnalysis::totalTaskTimeMs).sum();
        // Utilization = work done / capacity available over the SQL's wall clock, using the
        // actual core timeline when present (accurate under dynamic allocation).
        long capacityCoreMs = coreTimeline.coreMillis(sql.startTime(), sql.endTime());
        double util = capacityCoreMs <= 0 ? 0.0 : (double) totalTaskTime / (double) capacityCoreMs;

        return new SqlAnalysis(
                sql.executionId(), sql.statementId(), sql.description(), sql.physicalPlanText(),
                wall, criticalPath, ideal, deviation, util, taskConcurrency(sql), avgTaskConcurrency(sql), stageAnalyses,
                LimitProbeSummary.from(sql, app.jobs(), stagesById));
    }

    private int taskConcurrency(SqlExecution sql) {
        int max = 0;
        int active = 0;
        List<TaskEvent> events = taskEvents(sql);
        for (TaskEvent event : events) {
            active += event.delta;
            if (active > max) {
                max = active;
            }
        }
        return max;
    }

    private double avgTaskConcurrency(SqlExecution sql) {
        if (sql.startTime() <= 0L || sql.endTime() <= sql.startTime()) {
            return 0.0;
        }
        List<TaskEvent> events = taskEvents(sql);
        if (events.isEmpty()) {
            return 0.0;
        }
        long prev = sql.startTime();
        long busyMs = 0L;
        int active = 0;
        for (TaskEvent event : events) {
            long time = Math.max(sql.startTime(), Math.min(sql.endTime(), event.time));
            if (time > prev && active > 0) {
                busyMs += (long) active * (time - prev);
            }
            active += event.delta;
            prev = time;
        }
        return (double) busyMs / (double) (sql.endTime() - sql.startTime());
    }

    private List<TaskEvent> taskEvents(SqlExecution sql) {
        List<TaskEvent> events = new ArrayList<TaskEvent>();
        for (TaskInterval task : app.taskIntervals()) {
            if (task.sqlExecutionId() != null && task.sqlExecutionId().longValue() == sql.executionId()
                    && task.finishTime() > task.launchTime()) {
                events.add(new TaskEvent(task.launchTime(), 1));
                events.add(new TaskEvent(task.finishTime(), -1));
            }
        }
        events.sort((a, b) -> a.time == b.time ? Integer.compare(a.delta, b.delta) : Long.compare(a.time, b.time));
        return events;
    }

    private static final class TaskEvent {
        final long time;
        final int delta;
        TaskEvent(long time, int delta) { this.time = time; this.delta = delta; }
    }

    // ---- SQL -> stages linkage -------------------------------------------------

    private List<Stage> stagesFor(SqlExecution sql) {
        Set<Long> jobIds = new HashSet<>(sql.jobIds());
        Set<Integer> stageIds = new HashSet<>();
        for (Job j : app.jobs()) {
            boolean belongs = (j.sqlExecutionId() != null && j.sqlExecutionId() == sql.executionId())
                    || jobIds.contains((long) j.jobId());
            if (belongs) {
                stageIds.addAll(j.stageIds());
            }
        }
        List<Stage> result = new ArrayList<>();
        for (Integer id : stageIds) {
            Stage s = stagesById.get(id);
            if (s != null) {
                result.add(s);
            }
        }
        return result;
    }

    // ---- Critical path over the stage DAG --------------------------------------

    /**
     * Longest path through the stage DAG, where each node's weight is its max-task time
     * (the part you cannot remove by adding executors). Edges come from parent stage ids.
     * Computed with memoized DFS; cycles are impossible in a Spark stage DAG.
     */
    private long criticalPath(List<Stage> stages) {
        Map<Integer, Stage> local = new HashMap<>();
        for (Stage s : stages) local.put(s.stageId(), s);
        Map<Integer, Long> memo = new HashMap<>();
        long best = 0;
        for (Stage s : stages) {
            best = Math.max(best, longestTo(s.stageId(), local, memo, MetricAggregator::stageMaxTask));
        }
        return best;
    }

    private long idealTime(List<Stage> stages) {
        Map<Integer, Stage> local = new HashMap<>();
        for (Stage s : stages) local.put(s.stageId(), s);
        Map<Integer, Long> memo = new HashMap<>();
        long best = 0;
        for (Stage s : stages) {
            best = Math.max(best, longestTo(s.stageId(), local, memo, this::stageIdealTime));
        }
        return best;
    }

    private interface StageWeight {
        long weight(Stage s);
    }

    private static long stageMaxTask(Stage s) {
        return s.taskStats().durationMs().max();
    }

    private long stageIdealTime(Stage s) {
        long total = s.taskStats().durationMs().sum();
        return total / Math.max(1, availableCores);
    }

    private long longestTo(int stageId, Map<Integer, Stage> local,
                           Map<Integer, Long> memo, StageWeight w) {
        Long cached = memo.get(stageId);
        if (cached != null) return cached;
        Stage s = local.get(stageId);
        if (s == null) {
            memo.put(stageId, 0L);
            return 0L;
        }
        long parentMax = 0;
        for (Integer p : s.parentStageIds()) {
            if (local.containsKey(p)) {
                parentMax = Math.max(parentMax, longestTo(p, local, memo, w));
            }
        }
        long result = parentMax + w.weight(s);
        memo.put(stageId, result);
        return result;
    }

    // ---- Config ----------------------------------------------------------------

    private static int readAvailableCores(Map<String, String> conf) {
        int instances = parseInt(conf.get("spark.executor.instances"), 0);
        int cores = parseInt(conf.get("spark.executor.cores"), 1);
        int total = instances * cores;
        return total > 0 ? total : 1;
    }

    private static int parseInt(String v, int dflt) {
        if (v == null) return dflt;
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return dflt;
        }
    }
}
