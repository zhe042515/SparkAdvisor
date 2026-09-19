package io.sparkadvisor.core.analyze;

import io.sparkadvisor.core.metrics.Distribution;
import io.sparkadvisor.core.model.*;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class LimitProbeSummaryTest {
    @Test void countsRealProbeBatchesAndUsesMinimumConcurrentCores() {
        Fixture f = new Fixture();
        LimitProbeSummary summary = f.summary();
        assertNotNull(summary);
        assertEquals(3, summary.rounds());
        assertEquals(25, summary.scannedPartitions());
        assertEquals(16, summary.executorCores());
        assertEquals(4, summary.scaleUpFactor());
    }

    @Test void completedQueryInRunningApplicationFlowsThroughAggregator() {
        Fixture f = new Fixture();
        ApplicationModel app = new ApplicationModel("app", "test", 1, 0, true, Map.of(),
                List.of(f.sql()), f.jobs, new ArrayList<>(f.stages.values()), List.of(), List.of());
        assertEquals(3, new MetricAggregator(app).analyze(f.sql()).limitProbe().rounds());
    }

    @Test void otherJobsUnknownMetadataOverlapsAndRetriesAreNotRounds() {
        Fixture f = new Fixture();
        f.jobs.set(1, new Job(2, 7L, List.of(2), 300, 390, false));
        assertNull(f.summary());
        f = new Fixture();
        f.jobs.set(1, new Job(2, 7L, List.of(2), 300, 390, false, new LimitProbe(99, 100, 4, 1, 4, 32)));
        assertNull(f.summary());
        f = new Fixture();
        f.jobs.set(1, new Job(2, 7L, List.of(2), 150, 390, false, f.jobs.get(1).limitProbe()));
        assertNull(f.summary());
        f = new Fixture();
        f.stages.put(2, stage(2, 1, 4, 4));
        assertNull(f.summary());
    }

    @Test void planAndCompletionMustSupportTakeAndMissingTasksAreRejected() {
        Fixture f = new Fixture();
        for (String plan : List.of("TakeOrderedAndProject(limit=10)", "CollectLimit 10\n+- Exchange", "CollectLimit 10\n+- Subquery")) {
            f.plan = plan;
            assertNull(f.summary());
        }
        f = new Fixture();
        f.stages.put(2, stage(2, 0, 4, 3));
        assertNull(f.summary());
        f = new Fixture();
        f.end = 0;
        assertNull(f.summary());
        f = new Fixture();
        SqlExecution failed = new SqlExecution(7, null, "select * from t limit 10", f.plan,
                100, 600, false, List.of(1L, 2L, 3L), true);
        assertNull(LimitProbeSummary.from(failed, f.jobs, f.stages));
    }

    @Test void missingInitialValueUsesFirstBatchAndConflictingConfigsAreRejected() {
        Fixture f = new Fixture();
        for (int i = 0; i < f.jobs.size(); i++) {
            Job job = f.jobs.get(i);
            LimitProbe p = job.limitProbe();
            f.jobs.set(i, new Job(job.jobId(), 7L, job.stageIds(), job.submissionTime(), job.completionTime(), false,
                    new LimitProbe(10, 100, p.partitions(), 0, 0, p.executorCores())));
        }
        assertEquals(1, f.summary().initialPartitions());
        assertEquals("FIRST_PROBE_OBSERVED", f.summary().initialSource());
        assertEquals(0, f.summary().scaleUpFactor());
        f.jobs.set(1, new Job(2, 7L, List.of(2), 300, 390, false, new LimitProbe(10, 100, 4, 2, 4, 32)));
        assertNull(f.summary());
    }

    private static Stage stage(int id, int attempt, int tasks, int observed) {
        Distribution duration = new Distribution(observed, 10, 10, 10, 10, 10, 10, observed * 10L);
        TaskMetricStats stats = new TaskMetricStats(duration, Distribution.EMPTY, Distribution.EMPTY,
                Distribution.EMPTY, Distribution.EMPTY, Distribution.EMPTY, Distribution.EMPTY, Distribution.EMPTY, Distribution.EMPTY);
        return new Stage(id, attempt, tasks, List.of(), id * 100L + 100, id * 100L + 110,
                id * 100L + 190, 0, 0, stats);
    }

    private static class Fixture {
        String plan = "CollectLimit 10\n+- *(1) Filter (id > 100)\n   +- Range (0, 1000, step=1, splits=100)";
        long end = 600;
        final List<Job> jobs = new ArrayList<>();
        final Map<Integer, Stage> stages = new LinkedHashMap<>();
        Fixture() {
            int[] batches = {1, 4, 20};
            for (int i = 0; i < batches.length; i++) {
                int id = i + 1;
                jobs.add(new Job(id, 7L, List.of(id), id * 100L + 100, id * 100L + 190, false,
                        new LimitProbe(10, 100, batches[i], 1, 4, i == 1 ? 16 : 32)));
                stages.put(id, stage(id, 0, batches[i], batches[i]));
            }
        }
        SqlExecution sql() { return new SqlExecution(7, null, "select * from t limit 10", plan, 100, end, end == 0, List.of(1L, 2L, 3L)); }
        LimitProbeSummary summary() { return LimitProbeSummary.from(sql(), jobs, stages); }
    }
}
