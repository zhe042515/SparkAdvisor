package io.sparkadvisor.analyzer.v2;

import io.sparkadvisor.core.finding.Finding;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class LimitInitialPartitionsRuleTest {
    private final RuleEngineV2 engine = RuleEngineV2.sqlDefaults(RuleThresholdsV2.defaults());

    @Test void twoRoundsDoNotTriggerButThreeDoWithoutADurationThreshold() {
        assertTrue(findings(2, 1, 5, 4, 32, 4).isEmpty());
        Finding finding = findings(3, 1, 25, 4, 32, 4).get(0);
        assertEquals("5", finding.evidence().get("limit.suggested_initial_partitions"));
        assertEquals("true", finding.evidence().get("limit.exceeds_two_round_budget"));
        assertEquals(1, finding.recommendations().size());
    }

    @Test void observedCoreCapAlwaysWinsAndCannotRecommendADecrease() {
        assertEquals("16", findings(4, 1, 125, 4, 16, 4).get(0)
                .evidence().get("limit.suggested_initial_partitions"));
        assertTrue(findings(3, 8, 200, 4, 8, 32).get(0).recommendations().isEmpty());
        assertTrue(findings(3, 8, 200, 4, 4, 32).get(0).recommendations().isEmpty());
        assertTrue(findings(3, 1, 25, 4, 0, 4).get(0).recommendations().isEmpty());
    }

    @Test void usesLoggedFactorWithSparkLowerBoundAndNoAssumedDefault() {
        assertEquals("3", findings(3, 1, 9, 1, 32, 2).get(0)
                .evidence().get("limit.suggested_initial_partitions"));
        assertEquals("9", findings(3, 1, 81, 8, 32, 8).get(0)
                .evidence().get("limit.suggested_initial_partitions"));
        Finding missing = findings(3, 1, 25, 0, 32, 4).get(0);
        assertEquals("4", missing.evidence().get("limit.suggested_initial_partitions"));
        assertTrue(missing.caveat().contains("missing"));
    }

    @Test void roundsTriggerEvenWhenScannedSizeIsWithinTheTwoRoundBudget() {
        assertEquals("2", findings(3, 1, 3, 4, 32, 1).get(0)
                .evidence().get("limit.suggested_initial_partitions"));
    }

    @Test void arithmeticDoesNotOverflowAtIntegerLimits() {
        assertEquals("2147483646", findings(3, Integer.MAX_VALUE - 2, Integer.MAX_VALUE,
                Integer.MAX_VALUE, Integer.MAX_VALUE, 1).get(0)
                .evidence().get("limit.suggested_initial_partitions"));
    }

    @Test void missingEvidenceIsUnavailableAndCompletedSqlInLiveAppStillWorks() {
        RuleRunResult result = engine.evaluateDetailed(Collections.singletonList(MetricsContext.builder(RuleScope.SQL).build()));
        assertTrue(result.unavailableRules().get("S-30").contains(Capability.LIMIT_PROBE_METRICS));
        assertFalse(findings(3, 1, 25, 4, 32, 4).isEmpty()); // fixture marks app partial, not the closed SQL
    }

    private List<Finding> findings(int rounds, int initial, int scanned, int factor, int cores, int second) {
        MetricsContext context = MetricsContext.builder(RuleScope.SQL).executionId(7).partial(true)
                .capability(Capability.LIMIT_PROBE_METRICS)
                .number("limit.rounds", rounds).number("limit.initial_partitions", initial)
                .number("limit.scanned_partitions", scanned).number("limit.scale_up_factor", factor)
                .number("limit.executor_cores", cores).number("limit.second_round_partitions", second)
                .attribute("limit.initial_source", "JOB_PROPERTIES").build();
        return engine.evaluate(Collections.singletonList(context));
    }
}
