package io.sparkadvisor.core.model;

import io.sparkadvisor.core.util.ValueObjects;

import java.util.List;
import java.util.Objects;

public final class SqlExecution {
    private final long executionId;
    private final String statementId;
    private final String description;
    private final String physicalPlanText;
    private final long startTime;
    private final long endTime;
    private final boolean incomplete;
    private final boolean failed;
    private final List<Long> jobIds;

    public SqlExecution(long executionId, String statementId, String description, String physicalPlanText,
                        long startTime, long endTime, boolean incomplete, List<Long> jobIds) {
        this(executionId, statementId, description, physicalPlanText, startTime, endTime, incomplete, jobIds, false);
    }

    public SqlExecution(long executionId, String statementId, String description, String physicalPlanText,
                        long startTime, long endTime, boolean incomplete, List<Long> jobIds, boolean failed) {
        this.executionId = executionId;
        this.statementId = statementId;
        this.description = description;
        this.physicalPlanText = physicalPlanText;
        this.startTime = startTime;
        this.endTime = endTime;
        this.incomplete = incomplete;
        this.failed = failed;
        this.jobIds = jobIds;
    }
    public long executionId() { return executionId; }
    public String statementId() { return statementId; }
    public String description() { return description; }
    public String physicalPlanText() { return physicalPlanText; }
    public long startTime() { return startTime; }
    public long endTime() { return endTime; }
    public boolean incomplete() { return incomplete; }
    public boolean failed() { return failed; }
    public List<Long> jobIds() { return jobIds; }
    public long wallClockMs() { return (startTime <= 0 || endTime <= 0) ? 0 : endTime - startTime; }
    @Override public boolean equals(Object o) { if (this == o) return true; if (!(o instanceof SqlExecution)) return false; SqlExecution that = (SqlExecution) o; return executionId == that.executionId && startTime == that.startTime && endTime == that.endTime && incomplete == that.incomplete && failed == that.failed && Objects.equals(statementId, that.statementId) && Objects.equals(description, that.description) && Objects.equals(physicalPlanText, that.physicalPlanText) && Objects.equals(jobIds, that.jobIds); }
    @Override public int hashCode() { return Objects.hash(executionId, statementId, description, physicalPlanText, startTime, endTime, incomplete, jobIds, failed); }
    @Override public String toString(){return ValueObjects.toString(this);}
}
