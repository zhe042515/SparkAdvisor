package io.sparkadvisor.core.eventlog;

import io.sparkadvisor.core.model.ApplicationModel;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparkEventCollectorRetentionTest {

    @Test void keepsOnlyLatestCompletedSqlAndCapsLargeText() {
        EventRetentionPolicy policy = new EventRetentionPolicy(
                2, 10, 10, 10, 10, 10, 8, 12, false);
        SparkEventCollector collector = replay(sql(1, "first-description", "first-physical-plan")
                + sql(2, "second-description", "second-physical-plan")
                + sql(3, "third-description", "third-physical-plan"), policy);

        ApplicationModel app = collector.build();
        assertEquals(java.util.Arrays.asList(2L, 3L), app.sqlExecutions().stream()
                .map(q -> q.executionId()).collect(Collectors.toList()));
        assertEquals(8, app.sqlExecutions().get(0).description().length());
        assertEquals(12, app.sqlExecutions().get(0).physicalPlanText().length());
        assertTrue(collector.retentionSummary().contains("sql=1"));
    }

    @Test void rejectsNegativeLimits() {
        assertThrows(IllegalArgumentException.class, () -> new EventRetentionPolicy(
                -1, 1, 1, 1, 1, 1, 1, 1, false));
    }

    @Test void boundsCompletedJobsAndCompactsExecutorHistory() throws Exception {
        EventRetentionPolicy policy = new EventRetentionPolicy(
                10, 1, 10, 10, 1, 10, 1_000, 1_000, false);
        SparkEventCollector collector = replay(fixture(), policy);

        ApplicationModel app = collector.build();
        assertEquals(1, app.jobs().size());
        assertEquals(2, app.jobs().get(0).jobId());
        assertEquals(java.util.Collections.singletonList(2L), app.sqlExecutions().get(0).jobIds());
        assertEquals(2, app.executorEvents().size()); // compacted baseline + latest event
        assertTrue(collector.retentionSummary().contains("jobs=1"));
        assertTrue(collector.retentionSummary().contains("executorEvents=2"));
    }

    private static SparkEventCollector replay(String events, EventRetentionPolicy policy) {
        SparkEventCollector collector = new SparkEventCollector(false, policy);
        ReplayListenerBusAdapter bus = ReplayListenerBusAdapter.create();
        bus.addListener(collector);
        bus.replay(new ByteArrayInputStream(events.getBytes(StandardCharsets.UTF_8)),
                "retention-fixture", false);
        return collector;
    }

    private static String sql(long id, String description, String plan) {
        return "{\"Event\":\"org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart\"," 
                + "\"executionId\":" + id + ",\"rootExecutionId\":" + id + ","
                + "\"description\":\"" + description + "\",\"details\":\"\","
                + "\"physicalPlanDescription\":\"" + plan + "\","
                + "\"sparkPlanInfo\":{\"nodeName\":\"Project\",\"simpleString\":\"Project\","
                + "\"children\":[],\"metadata\":{},\"metrics\":[]},\"time\":" + (id * 10)
                + ",\"modifiedConfigs\":{}}\n"
                + "{\"Event\":\"org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd\","
                + "\"executionId\":" + id + ",\"time\":" + (id * 10 + 5)
                + ",\"errorMessage\":\"\"}\n";
    }

    private static String fixture() throws Exception {
        try (InputStream in = SparkEventCollectorRetentionTest.class
                .getResourceAsStream("/limit-probe-events.jsonl")) {
            assertNotNull(in);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
