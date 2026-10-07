package io.sparkadvisor.analyzer.v2;

import io.sparkadvisor.core.finding.Finding;
import io.sparkadvisor.core.finding.Recommendation;
import io.sparkadvisor.core.finding.Severity;
import io.sparkadvisor.core.model.LimitProbe;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** S-30: more than two LIMIT probes warrants a larger, core-capped initial batch. */
public final class LimitInitialPartitionsRule implements MetricRule {
    public String id() { return "S-30"; }
    public RuleScope scope() { return RuleScope.SQL; }
    public Set<Capability> requires() { return Collections.singleton(Capability.LIMIT_PROBE_METRICS); }
    public Set<String> thresholdKeys() { return Collections.singleton("limit.max_rounds"); }

    public List<Finding> evaluate(MetricsContext context, RuleThresholdsV2 thresholds) {
        long rounds = metric(context, "limit.rounds");
        long initial = metric(context, "limit.initial_partitions");
        long scanned = metric(context, "limit.scanned_partitions");
        long cores = metric(context, "limit.executor_cores");
        long factor = metric(context, "limit.scale_up_factor");
        long second = metric(context, "limit.second_round_partitions");
        if (rounds <= thresholds.get("limit.max_rounds") || initial <= 0 || scanned <= 0
                || cores < 0 || factor < 0 || second <= 0) return Collections.emptyList();

        // Spark uses max(2, configured factor). This is a two-round sizing heuristic,
        // not a promise: rows found in earlier batches change Spark's next-batch estimate.
        long effectiveFactor = Math.max(2L, factor);
        long target = factor > 0 ? (scanned + effectiveFactor) / (1L + effectiveFactor) : second;
        long suggested = Math.min(cores, Math.max(initial + 1L, target));
        Map<String, String> evidence = new LinkedHashMap<String, String>();
        evidence.put("limit.rounds", Long.toString(rounds));
        evidence.put("limit.scanned_partitions", Long.toString(scanned));
        evidence.put(LimitProbe.INITIAL_PARTITIONS, Long.toString(initial));
        evidence.put("limit.initial_source", context.attribute("limit.initial_source"));
        evidence.put(LimitProbe.SCALE_UP_FACTOR, factor > 0 ? Long.toString(factor) : "UNKNOWN");
        evidence.put("limit.executor_cores", Long.toString(cores));
        evidence.put("limit.core_source", "MIN_OBSERVED_AT_PROBE_START");
        if (factor > 0) {
            evidence.put("limit.two_round_budget", Long.toString(initial * (1L + effectiveFactor)));
            evidence.put("limit.exceeds_two_round_budget", Boolean.toString(scanned > initial * (1L + effectiveFactor)));
        }

        List<Recommendation> recommendations = Collections.emptyList();
        String caveat;
        if (cores == 0) {
            caveat = "Executor core count is unknown; verify the total before choosing a larger initial batch.";
        } else if (suggested <= initial) {
            caveat = "The current value already reaches the observed executor core cap; no larger value is recommended.";
        } else {
            evidence.put("limit.suggested_initial_partitions", Long.toString(suggested));
            recommendations = Collections.singletonList(Recommendation.session(
                    "SET " + LimitProbe.INITIAL_PARTITIONS + "=" + suggested + ";",
                    "LIMIT required more than two probe rounds.",
                    "Try a larger first batch, capped by observed executor cores; compare the next execution."));
            caveat = factor > 0
                    ? "Two-round sizing uses observed partitions / (1 + max(2, scaleUpFactor)); it does not guarantee two rounds or a speedup."
                    : "scaleUpFactor is missing; use the observed second batch instead of assuming a default.";
        }
        Finding finding = new Finding(id(), "limit", recommendations.isEmpty() ? Severity.INFO : Severity.WARN,
                null, "LIMIT required more than two probe rounds; review initialNumPartitions.", evidence,
                recommendations).withQuality(recommendations.isEmpty() ? Severity.INFO : Severity.WARN, "MEDIUM", caveat);
        return Collections.singletonList(finding);
    }

    private static long metric(MetricsContext context, String key) {
        if (!context.has(key)) return -1L;
        double value = context.number(key);
        return Double.isNaN(value) || Double.isInfinite(value) || value < 0 || value > Integer.MAX_VALUE
                || value != Math.rint(value) ? -1L : (long) value;
    }
}
