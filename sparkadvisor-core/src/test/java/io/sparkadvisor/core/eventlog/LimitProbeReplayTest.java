package io.sparkadvisor.core.eventlog;

import io.sparkadvisor.core.model.ApplicationModel;
import io.sparkadvisor.core.model.LimitProbe;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class LimitProbeReplayTest {
    @Test void officialReplayRetainsJobPropertiesRddAndConcurrentExecutorCores() throws Exception {
        ApplicationModel app = replay(fixture());
        assertEquals(2, app.jobs().size());
        LimitProbe first = app.jobs().get(0).limitProbe();
        assertNotNull(first);
        assertEquals(10, first.rddId());
        assertEquals(100, first.totalPartitions());
        assertEquals(1, first.partitions());
        assertEquals(1, first.initialPartitions());
        assertEquals(4, first.scaleUpFactor());
        assertEquals(16, first.executorCores());
        assertEquals(8, app.jobs().get(1).limitProbe().executorCores());
        assertEquals(250, app.jobs().get(0).completionTime());
        assertTrue(app.incomplete()); // application still running
        assertFalse(app.sqlExecutions().get(0).incomplete());
        assertFalse(app.sqlExecutions().get(0).failed());
    }

    @Test void preservesUnknownAndRedactedConfigsInsteadOfInventingDefaults() throws Exception {
        String content = fixture().replace("\"spark.sql.limit.scaleUpFactor\":\"4\"", "\"unrelated\":\"4\"")
                .replace("\"spark.sql.limit.initialNumPartitions\":\"1\"", "\"spark.sql.limit.initialNumPartitions\":\"********\"");
        LimitProbe probe = replay(content).jobs().get(0).limitProbe();
        assertEquals(0, probe.scaleUpFactor());
        assertEquals(-1, probe.initialPartitions());
    }

    @Test void recordsSqlFailureAndDoesNotCollectUnrelatedQueries() throws Exception {
        assertTrue(replay(fixture().replace("\"errorMessage\":\"\"", "\"errorMessage\":\"query failed\""))
                .sqlExecutions().get(0).failed());
        assertNull(replay(fixture().replace("CollectLimit", "Project")).jobs().get(0).limitProbe());
    }

    private static String fixture() throws Exception {
        try (InputStream in = LimitProbeReplayTest.class.getResourceAsStream("/limit-probe-events.jsonl")) {
            assertNotNull(in);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
    private static ApplicationModel replay(String content) {
        return new EventLogParser().parse(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)), "limit-fixture", false);
    }
}
