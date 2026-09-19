package io.sparkadvisor.analyzer.v2;

/** Optional evidence sources declared by rules.md section 2.3. */
public enum Capability {
    BASE_TASK_METRICS,
    STAGE_EXECUTOR_METRICS,
    PLAN_METRICS,
    PLAN_TEXT,
    LIMIT_PROBE_METRICS,
    BASELINE,
    STATEMENT_ID,
    QUEUE_TIMELINE,
    HOST_METRICS,
    NETWORK_MATRIX,
    DATA_QUALITY
}
