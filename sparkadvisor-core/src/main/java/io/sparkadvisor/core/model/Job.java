package io.sparkadvisor.core.model;

import io.sparkadvisor.core.util.ValueObjects;

import java.util.List;
import java.util.Objects;

public final class Job {
    private final int jobId;
    private final Long sqlExecutionId;
    private final List<Integer> stageIds;
    private final long submissionTime;
    private final long completionTime;
    private final boolean failed;
    private final LimitProbe limitProbe;

    public Job(int jobId, Long sqlExecutionId, List<Integer> stageIds, long submissionTime, long completionTime, boolean failed) {
        this(jobId, sqlExecutionId, stageIds, submissionTime, completionTime, failed, null);
    }

    public Job(int jobId, Long sqlExecutionId, List<Integer> stageIds, long submissionTime,
               long completionTime, boolean failed, LimitProbe limitProbe) {
        this.jobId = jobId;
        this.sqlExecutionId = sqlExecutionId;
        this.stageIds = stageIds;
        this.submissionTime = submissionTime;
        this.completionTime = completionTime;
        this.failed = failed;
        this.limitProbe = limitProbe;
    }

    public int jobId() { return jobId; }
    public Long sqlExecutionId() { return sqlExecutionId; }
    public List<Integer> stageIds() { return stageIds; }
    public long submissionTime() { return submissionTime; }
    public long completionTime() { return completionTime; }
    public boolean failed() { return failed; }
    public LimitProbe limitProbe() { return limitProbe; }

    public long wallClockMs() { return (submissionTime <= 0 || completionTime <= 0) ? 0 : completionTime - submissionTime; }

    @Override public boolean equals(Object o) { if (this == o) return true; if (!(o instanceof Job)) return false; Job job = (Job) o; return jobId == job.jobId && submissionTime == job.submissionTime && completionTime == job.completionTime && failed == job.failed && Objects.equals(sqlExecutionId, job.sqlExecutionId) && Objects.equals(stageIds, job.stageIds) && Objects.equals(limitProbe, job.limitProbe); }
    @Override public int hashCode() { return Objects.hash(jobId, sqlExecutionId, stageIds, submissionTime, completionTime, failed, limitProbe); }
    @Override public String toString(){return ValueObjects.toString(this);}
}
