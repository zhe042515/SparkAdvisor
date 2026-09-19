package io.sparkadvisor.report;

import io.sparkadvisor.core.metrics.Distribution;
import io.sparkadvisor.core.model.*;
import io.sparkadvisor.report.html.HtmlReportWriter;
import io.sparkadvisor.report.json.JsonReportWriter;
import io.sparkadvisor.report.model.AnalysisResult;
import io.sparkadvisor.report.model.AnalysisResultBuilder;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class LimitRecommendationReportTest {
    @Test void sharedReportPipelineExposesCoreCappedRecommendationInJsonAndHtml() throws Exception {
        SqlExecution sql = new SqlExecution(7, "limit-test", "select * from t limit 10",
                "CollectLimit 10\n+- Range (0, 1000, step=1, splits=100)", 100, 600, false, List.of(1L, 2L, 3L));
        List<Job> jobs = new ArrayList<>();
        List<Stage> stages = new ArrayList<>();
        int[] batches = {1, 4, 20};
        for (int i = 0; i < batches.length; i++) {
            int id = i + 1, count = batches[i];
            jobs.add(new Job(id, 7L, List.of(id), id * 100L + 100, id * 100L + 190, false,
                    new LimitProbe(10, 100, count, 1, 4, 4)));
            Distribution duration = new Distribution(count, 10, 10, 10, 10, 10, 10, count * 10L);
            TaskMetricStats stats = new TaskMetricStats(duration, Distribution.EMPTY, Distribution.EMPTY,
                    Distribution.EMPTY, Distribution.EMPTY, Distribution.EMPTY, Distribution.EMPTY, Distribution.EMPTY, Distribution.EMPTY);
            stages.add(new Stage(id, 0, count, List.of(), id * 100L + 100, id * 100L + 110, id * 100L + 190, 0, 0, stats));
        }
        // Live app remains open; SQL and its jobs are complete. Startup conf must not override observed cores.
        ApplicationModel app = new ApplicationModel("app", "test", 1, 0, true,
                Map.of("spark.executor.instances", "100", "spark.executor.cores", "8"),
                List.of(sql), jobs, stages, List.of(), List.of());
        AnalysisResult result = new AnalysisResultBuilder(app, "live://app").build(sql);
        var finding = result.findings().stream().filter(f -> "S-30".equals(f.ruleId())).findFirst().orElseThrow();
        assertEquals("4", finding.evidence().get("limit.suggested_initial_partitions"));
        assertEquals("SET spark.sql.limit.initialNumPartitions=4;", finding.recommendations().get(0).action());
        String json = new JsonReportWriter().toJson(result);
        assertTrue(json.contains("S-30"));
        assertTrue(json.contains("limit.suggested_initial_partitions"));
        String html = new HtmlReportWriter().render(result, true);
        assertTrue(html.contains("LIMIT 实际取数 3 轮"));
        assertTrue(html.contains("spark.sql.limit.initialNumPartitions=4;"));
    }
}
