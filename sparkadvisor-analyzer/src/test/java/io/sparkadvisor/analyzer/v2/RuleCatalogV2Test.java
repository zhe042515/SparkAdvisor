package io.sparkadvisor.analyzer.v2;

import io.sparkadvisor.core.finding.Finding;
import io.sparkadvisor.core.finding.Recommendation;
import io.sparkadvisor.core.finding.Severity;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RuleCatalogV2Test {

    @Test
    void catalogHasStable50IdsAndEveryRuleHasAGoldenTrigger() {
        List<MetricRule> rules = RuleCatalogV2.all();
        assertEquals(50, rules.size());
        assertEquals(30, rules.stream().filter(r -> r.id().startsWith("S-")).count());
        assertEquals(18, rules.stream().filter(r -> r.id().startsWith("Q-")).count());
        assertEquals(2, rules.stream().filter(r -> r.id().startsWith("DQ-")).count());

        RuleEngineV2 engine = new RuleEngineV2(rules, RuleThresholdsV2.defaults(), Collections.emptyList());
        assertTrue(engine.unusedThresholdKeys().isEmpty(), "default threshold registry must match catalog declarations: " + engine.unusedThresholdKeys());
        List<Finding> findings = engine.evaluate(Arrays.asList(sqlFixture(), stageFixture(false), queueFixture(), dqFixture()));
        Set<String> actual = findings.stream().map(Finding::ruleId).collect(Collectors.toSet());
        Set<String> expected = rules.stream().map(MetricRule::id).collect(Collectors.toSet());
        Set<String> documented = new HashSet<>();
        for (int i = 1; i <= 30; i++) documented.add(String.format("S-%02d", i));
        for (int i = 1; i <= 18; i++) documented.add(String.format("Q-%02d", i));
        documented.add("DQ-01"); documented.add("DQ-02");
        assertEquals(documented, expected);
        assertEquals(expected, actual, "the combined golden fixture must exercise every documented rule");
        for (MetricRule rule : rules) {
            MetricsContext fixture = rule.scope() == RuleScope.SQL ? sqlFixture()
                    : rule.scope() == RuleScope.STAGE ? stageFixture(false)
                    : rule.scope() == RuleScope.QUEUE ? queueFixture() : dqFixture();
            assertFalse(rule.evaluate(fixture, RuleThresholdsV2.defaults()).isEmpty(),
                    "missing isolated golden trigger for " + rule.id());
        }
    }

    @Test
    void recommendationsUseTheFourDocumentedActionTypes() {
        Set<Recommendation.Type> types = new HashSet<>();
        for (Finding finding : new RuleEngineV2(RuleCatalogV2.all(), RuleThresholdsV2.defaults(), Collections.emptyList())
                .evaluate(Arrays.asList(sqlFixture(), stageFixture(false), queueFixture(), dqFixture()))) {
            finding.recommendations().forEach(r -> types.add(r.type()));
        }
        assertTrue(types.contains(Recommendation.Type.SESSION_SET));
        assertTrue(types.contains(Recommendation.Type.RESTART_CONF));
        assertTrue(types.contains(Recommendation.Type.REWRITE));
        assertTrue(types.contains(Recommendation.Type.GOVERNANCE));
    }

    @Test
    void rulesMarkdownThresholdYamlMatchesRuntimeRegistry() throws Exception {
        java.nio.file.Path base = Paths.get(System.getProperty("basedir", ".")).toAbsolutePath();
        java.nio.file.Path rules = base.resolve("docs/rules.md");
        if (!java.nio.file.Files.exists(rules)) rules = base.getParent().resolve("docs/rules.md");
        RuleThresholdsV2 loaded = RuleThresholdsV2.fromYaml(rules);
        assertEquals(5.0, loaded.get("skew.ratio"));
        assertEquals(0.15, loaded.get("fetch_wait.ratio"));
        assertEquals(5000.0, loaded.get("dq.clock_skew_ms"));
        assertTrue(new RuleEngineV2(RuleCatalogV2.all(), loaded, Collections.emptyList())
                .unusedThresholdKeys().isEmpty());
    }

    @Test
    void capabilityMissingSkipsRuleInsteadOfTreatingMissingMetricsAsZero() {
        MetricsContext noPlanCapability = MetricsContext.builder(RuleScope.SQL)
                .number("limit.rounds", 3).number("limit.initial_partitions", 1).number("limit.scanned_partitions", 25)
                .number("limit.scale_up_factor", 4).number("limit.executor_cores", 32).number("limit.second_round_partitions", 4)
                .number("scan.files", 6000).number("scan.bytes", 1024).build();
        RuleRunResult run = RuleEngineV2.sqlDefaults(RuleThresholdsV2.defaults())
                .evaluateDetailed(Collections.singletonList(noPlanCapability));
        List<Finding> findings = run.findings();
        assertFalse(has(findings, "S-05"));
        assertFalse(has(findings, "S-19"));
        assertFalse(has(findings, "S-22"));
        assertTrue(run.unavailableRules().get("S-05").contains(Capability.PLAN_METRICS));
        assertTrue(run.unavailableRules().get("S-19").contains(Capability.PLAN_TEXT));
        assertTrue(run.unavailableRules().get("S-22").contains(Capability.BASELINE));
    }

    @Test
    void thresholdOverrideChangesBoundaryWithoutChangingCode() {
        RuleThresholdsV2 strict = RuleThresholdsV2.defaults().with("skew.min_tasks", 4000);
        List<Finding> findings = RuleEngineV2.sqlDefaults(strict)
                .evaluate(Collections.singletonList(stageFixture(false)));
        assertFalse(has(findings, "S-01"));
        assertTrue(has(findings, "S-02"));
    }

    @Test
    void partialStageCapsCriticalAtWarnAndLowersConfidence() {
        Finding finding = find(RuleEngineV2.sqlDefaults(RuleThresholdsV2.defaults())
                .evaluate(Collections.singletonList(stageFixture(true))), "S-01");
        assertNotNull(finding);
        assertEquals(Severity.WARN, finding.severity());
        assertEquals("MEDIUM", finding.confidence());
        assertTrue(finding.caveat().contains("Partial"));
    }

    @Test
    void suppressionKeepsFindingForAuditButRemovesItFromNormalPriority() {
        Suppression suppression = new Suppression("S-01", "fp-1", null, null,
                "known skew", LocalDate.now().plusDays(1));
        RuleEngineV2 engine = new RuleEngineV2(RuleCatalogV2.sqlAndDataQuality(),
                RuleThresholdsV2.defaults(), Collections.singletonList(suppression));
        Finding finding = find(engine.evaluate(Collections.singletonList(stageFixture(false))), "S-01");
        assertTrue(finding.suppressed());
        assertEquals("known skew", finding.suppressionReason());
    }

    @Test
    void queueAttributionDowngradesSelfTuningBeforeMicroOptimization() {
        RuleEngineV2 engine = new RuleEngineV2(RuleCatalogV2.all(), RuleThresholdsV2.defaults(), Collections.emptyList());
        Finding skew = find(engine.evaluate(Arrays.asList(sqlFixture(), stageFixture(false))), "S-01");
        assertEquals(Severity.WARN, skew.severity(), "S-14 queue attribution must cap the critical skew priority");
        assertTrue(skew.caveat().contains("Queue wait dominates"));
    }

    @Test
    void dataQualityClockSkewDowngradesHostAttribution() {
        RuleEngineV2 engine = new RuleEngineV2(RuleCatalogV2.all(), RuleThresholdsV2.defaults(), Collections.emptyList());
        Finding io = find(engine.evaluate(Arrays.asList(queueFixture(), dqFixture())), "Q-12");
        assertEquals(Severity.WARN, io.severity());
        assertEquals("LOW", io.confidence());
        assertTrue(io.caveat().contains("timestamps"));
    }

    @Test
    void cleanCoreFixtureDoesNotTriggerWarnOrCritical() {
        MetricsContext clean = MetricsContext.builder(RuleScope.STAGE)
                .capability(Capability.BASE_TASK_METRICS)
                .number("num_tasks", 10).number("task_duration.max_ms", 1000)
                .number("task_duration.p50_ms", 900).number("runtime.sum_ms", 5000)
                .number("gc.ratio", 0.01).number("shuffle_read.sum_bytes", 0)
                .number("spill.disk_sum_bytes", 0).build();
        List<Finding> findings = RuleEngineV2.sqlDefaults(RuleThresholdsV2.defaults())
                .evaluate(Collections.singletonList(clean));
        assertTrue(findings.stream().allMatch(f -> f.severity() == Severity.INFO));
    }

    private static MetricsContext sqlFixture() {
        return withAllCaps(MetricsContext.builder(RuleScope.SQL).executionId(42).attribute("fingerprint", "fp-1")
                .attribute("statement_id", "stmt").attribute("plan.has_smj", "true")
                .attribute("plan.has_shj", "false").attribute("plan.partition_filters_empty", "true")
                .attribute("aqe.changed_plan", "true").attribute("plan.has_cartesian", "true")
                .attribute("plan.has_bnlj", "true").attribute("baseline.plan_changed", "true")
                .number("limit.rounds", 3).number("limit.initial_partitions", 1).number("limit.scanned_partitions", 25)
                .number("limit.scale_up_factor", 4).number("limit.executor_cores", 32).number("limit.second_round_partitions", 4)
                .number("scan.files", 6000).number("scan.bytes", 6000L * 1024L * 1024L)
                .number("output.files", 1000).number("output.bytes", 1024L * 1024L * 1024L)
                .number("queue.wait_ratio", 0.60).number("queue.busy_ratio", 0.95)
                .number("duration_ms", 600000).number("driver_gap.ratio", 0.50)
                .number("critical_path_ms", 400000).number("jobs.count", 60).number("jobs.p50_ms", 1000)
                .number("join.small_side_bytes", 32L * 1024L * 1024L)
                .number("broadcast.bytes", 1024L * 1024L * 1024L).number("join.output_rows", 1_000_000_000L)
                .number("join.critical_path_ratio", 0.50).number("row_amp.ratio", 20)
                .number("row_amp.output_rows", 1_000_000_000L).number("failed_tasks", 2)
                .number("stage_retries", 1).number("speculative.tasks", 100)
                .number("baseline.samples", 10).number("duration.p50_ms", 1_000_000)
                .number("baseline.duration.p50_ms", 300_000).number("impact_wall_ms", 600_000));
    }

    private static MetricsContext stageFixture(boolean partial) {
        return withAllCaps(MetricsContext.builder(RuleScope.STAGE).executionId(42).stageId(7)
                .attribute("fingerprint", "fp-1").attribute("pure_shuffle_stage", "false")
                .attribute("plan.codegen_gap", "true").partial(partial)
                .number("num_tasks", 3000).number("alive_cores", 400)
                .number("task_duration.max_ms", 600000).number("task_duration.p50_ms", 1000)
                .number("task_duration.sum_ms", 600000).number("runtime.sum_ms", 600000)
                .number("shuffle_read.p50_bytes", 512L * 1024L * 1024L)
                .number("shuffle_read.max_bytes", 8L * 1024L * 1024L * 1024L)
                .number("shuffle_read.sum_bytes", 20L * 1024L * 1024L * 1024L)
                .number("scheduler_delay.sum_ms", 300000).number("scheduler_delay.p50_ms", 2000)
                .number("deserialize.sum_ms", 300000).number("deserialize.p95_ms", 6000)
                .number("shuffle_write.sum_bytes", 20L * 1024L * 1024L * 1024L)
                .number("shuffle_write.sum_ms", 1_000_000).number("shuffle_write.max_task_ratio", 0.4)
                .number("input.sum_bytes", 10L * 1024L * 1024L * 1024L)
                .number("spill.disk_sum_bytes", 20L * 1024L * 1024L * 1024L)
                .number("spill.memory_sum_bytes", 1024L * 1024L * 1024L)
                .number("gc.ratio", 0.25).number("heap.peak_ratio", 0.95)
                .number("cpu.ratio", 0.80).number("fetch_wait.ratio", 0.25)
                .number("result_ser.ratio", 0.20).number("result_size.sum_bytes", 2L * 1024L * 1024L * 1024L)
                .number("result_size.max_bytes", 512L * 1024L * 1024L)
                .number("locality.bad_ratio", 0.80).number("critical_path_ratio", 0.50)
                .number("impact_wall_ms", 600000));
    }

    private static MetricsContext queueFixture() {
        return withAllCaps(MetricsContext.builder(RuleScope.QUEUE)
                .number("idle_window_minutes", 60).number("overload_window_minutes", 60)
                .number("queue_hotspot_count", 3).number("monopoly.window_minutes", 20)
                .number("monopoly.core_ratio", 0.8).number("monopoly.concurrent_statements", 5)
                .number("systemic.norm_median", 1.8).number("systemic.concurrent", 5)
                .attribute("systemic.single_host", "false").number("host.max_score", 2.0)
                .number("host.max_samples", 100).number("host.max_stages", 5)
                .number("executor.removed_count", 2).number("heap.trend_slope", 0.1)
                .number("config.issue_count", 3).number("small_files.affected_queries", 10)
                .number("io.worst_host_ratio", 0.2).number("io.worst_host_tasks", 200)
                .number("network.fetch_failures", 100).number("network.src_row_ratio", 0.8)
                .number("memory.pressure_events", 5).number("driver.gc_ratio", 0.2)
                .number("storm.failed_tasks", 500).number("storm.fail_ratio", 0.2)
                .number("restart.interrupted_queries", 2).number("queue.duration_p50_ratio", 1.5)
                .number("queue.regressed_fraction", 0.5).number("impact_core_seconds", 10000));
    }

    private static MetricsContext dqFixture() {
        return withAllCaps(MetricsContext.builder(RuleScope.DATA_QUALITY)
                .number("partial_stage_count", 2).attribute("inprogress", "true")
                .number("negative_duration_tasks", 1).number("max_clock_skew_ms", 10000));
    }

    private static MetricsContext withAllCaps(MetricsContext.Builder builder) {
        for (Capability capability : Capability.values()) builder.capability(capability);
        return builder.build();
    }

    private static boolean has(List<Finding> findings, String id) { return find(findings, id) != null; }
    private static Finding find(List<Finding> findings, String id) {
        return findings.stream().filter(f -> id.equals(f.ruleId())).findFirst().orElse(null);
    }
}
