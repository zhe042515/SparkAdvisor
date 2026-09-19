package io.sparkadvisor.core.model;

import io.sparkadvisor.core.util.ValueObjects;

/** Small JobStart snapshot used to recognize repeated LIMIT probes without task details. */
public final class LimitProbe {
    public static final String INITIAL_PARTITIONS = "spark.sql.limit.initialNumPartitions";
    public static final String SCALE_UP_FACTOR = "spark.sql.limit.scaleUpFactor";

    private final int rddId, totalPartitions, partitions, initialPartitions, scaleUpFactor, executorCores;

    /** Config value 0 means absent from job properties; -1 means invalid or redacted. */
    public LimitProbe(int rddId, int totalPartitions, int partitions, int initialPartitions,
                      int scaleUpFactor, int executorCores) {
        this.rddId = rddId;
        this.totalPartitions = totalPartitions;
        this.partitions = partitions;
        this.initialPartitions = initialPartitions;
        this.scaleUpFactor = scaleUpFactor;
        this.executorCores = executorCores;
    }

    public int rddId() { return rddId; }
    public int totalPartitions() { return totalPartitions; }
    public int partitions() { return partitions; }
    public int initialPartitions() { return initialPartitions; }
    public int scaleUpFactor() { return scaleUpFactor; }
    public int executorCores() { return executorCores; }
    @Override public boolean equals(Object other) { return ValueObjects.equalFields(this, other); }
    @Override public int hashCode() { return ValueObjects.hashFields(this); }
}
