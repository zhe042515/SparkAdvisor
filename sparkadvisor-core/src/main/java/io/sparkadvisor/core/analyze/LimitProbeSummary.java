package io.sparkadvisor.core.analyze;

import io.sparkadvisor.core.model.Job;
import io.sparkadvisor.core.model.LimitProbe;
import io.sparkadvisor.core.model.SqlExecution;
import io.sparkadvisor.core.model.Stage;
import io.sparkadvisor.core.util.ValueObjects;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Recognizes serial probes of the same RDD; SQL job count alone is not a round count. */
public final class LimitProbeSummary {
    private static final Pattern COLLECT_LIMIT = Pattern.compile("(?m)^\\s*CollectLimit\\s+[1-9][0-9]*(?:\\s|$)");
    private static final Pattern OTHER_PATH = Pattern.compile(
            "(?i)Join|Exchange|Subquery|TakeOrdered|GlobalLimit|LocalLimit|offset|AdaptiveSparkPlan");
    private final int rounds, scannedPartitions, initialPartitions, scaleUpFactor, executorCores, secondRoundPartitions;
    private final String initialSource;

    private LimitProbeSummary(int rounds, int scanned, int initial, int factor, int cores,
                              int secondRound, String source) {
        this.rounds = rounds;
        this.scannedPartitions = scanned;
        this.initialPartitions = initial;
        this.scaleUpFactor = factor;
        this.executorCores = cores;
        this.secondRoundPartitions = secondRound;
        this.initialSource = source;
    }

    /** Returns null when the execution cannot be identified as a complete LIMIT probe sequence. */
    public static LimitProbeSummary from(SqlExecution sql, List<Job> jobs, Map<Integer, Stage> stages) {
        String plan = sql.physicalPlanText();
        if (sql.incomplete() || sql.failed() || sql.endTime() <= sql.startTime() || plan == null
                || !COLLECT_LIMIT.matcher(plan).find() || OTHER_PATH.matcher(plan).find()) return null;
        List<Job> probes = new ArrayList<Job>();
        for (Job job : jobs) {
            if (job.sqlExecutionId() != null && job.sqlExecutionId().longValue() == sql.executionId()) probes.add(job);
        }
        if (probes.isEmpty()) return null;
        probes.sort(Comparator.comparingLong(Job::submissionTime));
        LimitProbe first = probes.get(0).limitProbe();
        if (first == null || first.totalPartitions() <= 0 || first.initialPartitions() < 0
                || first.scaleUpFactor() < 0) return null;
        int initial = first.initialPartitions() == 0 ? first.partitions() : first.initialPartitions();
        if (initial <= 0 || first.partitions() != Math.min(initial, first.totalPartitions())) return null;
        long scanned = 0, previousEnd = sql.startTime();
        int cores = Integer.MAX_VALUE;
        Set<Integer> seenStages = new HashSet<Integer>();
        for (Job job : probes) {
            LimitProbe probe = job.limitProbe();
            if (probe == null || job.failed() || job.completionTime() <= job.submissionTime()
                    || job.submissionTime() < previousEnd || job.completionTime() > sql.endTime()
                    || job.stageIds().size() != 1 || !seenStages.add(job.stageIds().get(0))
                    || probe.rddId() != first.rddId() || probe.totalPartitions() != first.totalPartitions()
                    || probe.initialPartitions() != first.initialPartitions() || probe.scaleUpFactor() != first.scaleUpFactor()
                    || probe.partitions() <= 0) return null;
            Stage stage = stages.get(job.stageIds().get(0));
            if (stage == null || stage.attemptId() != 0 || stage.completionTime() <= 0
                    || stage.numTasks() != probe.partitions() || stage.failedTaskAttempts() > 0
                    || stage.extraTaskAttempts() > 0 || stage.taskStats() == null
                    || stage.taskStats().durationMs().count() != probe.partitions()) return null;
            if (scanned > 0 && first.scaleUpFactor() > 0
                    && probe.partitions() > scanned * Math.max(2L, first.scaleUpFactor())) return null;
            scanned += probe.partitions();
            if (scanned > first.totalPartitions()) return null;
            cores = Math.min(cores, probe.executorCores());
            previousEnd = job.completionTime();
        }
        return new LimitProbeSummary(probes.size(), (int) scanned, initial, first.scaleUpFactor(), cores,
                probes.size() > 1 ? probes.get(1).limitProbe().partitions() : 0,
                first.initialPartitions() > 0 ? "JOB_PROPERTIES" : "FIRST_PROBE_OBSERVED");
    }

    public int rounds() { return rounds; }
    public int scannedPartitions() { return scannedPartitions; }
    public int initialPartitions() { return initialPartitions; }
    public int scaleUpFactor() { return scaleUpFactor; }
    public int executorCores() { return executorCores; }
    public int secondRoundPartitions() { return secondRoundPartitions; }
    public String initialSource() { return initialSource; }
    @Override public boolean equals(Object other) { return ValueObjects.equalFields(this, other); }
    @Override public int hashCode() { return ValueObjects.hashFields(this); }
}
