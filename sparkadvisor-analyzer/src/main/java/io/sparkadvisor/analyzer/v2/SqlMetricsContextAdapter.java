package io.sparkadvisor.analyzer.v2;

import io.sparkadvisor.core.analyze.SqlAnalysis;
import io.sparkadvisor.core.analyze.LimitProbeSummary;
import io.sparkadvisor.core.analyze.StageAnalysis;
import io.sparkadvisor.core.util.Java8Collections;
import io.sparkadvisor.core.util.Strings;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Converts the metrics currently available from core into rules.md metric keys. */
public final class SqlMetricsContextAdapter {
    private SqlMetricsContextAdapter() {}

    public static List<MetricsContext> from(SqlAnalysis sql,Map<String,String> conf){return from(sql,conf,false);}

    public static List<MetricsContext> from(SqlAnalysis sql,Map<String,String> conf,boolean incomplete){
        List<MetricsContext> out=new ArrayList<MetricsContext>();
        int cores=parse(conf==null?null:conf.get("spark.executor.instances"),0)*parse(conf==null?null:conf.get("spark.executor.cores"),1);
        MetricsContext.Builder sqlBuilder=MetricsContext.builder(RuleScope.SQL).executionId(sql.executionId())
                .capability(Capability.BASE_TASK_METRICS).partial(incomplete)
                .number("duration_ms",sql.wallClockMs()).number("critical_path_ms",sql.criticalPathMs())
                .number("ideal_ms",sql.idealMs()).number("core_utilization",sql.coreUtilization())
                .number("impact_wall_ms",Math.max(0,sql.wallClockMs()-sql.criticalPathMs()))
                .attribute("statement_id",sql.statementId()).attribute("sql",sql.description());
        if(!Strings.isBlank(sql.statementId()))sqlBuilder.capability(Capability.STATEMENT_ID);
        String plan=sql.physicalPlanText();
        if(!Strings.isBlank(plan)){
            String lower=plan.toLowerCase(Locale.ROOT);
            sqlBuilder.capability(Capability.PLAN_TEXT).attribute("plan",plan)
                    .attribute("plan.has_smj",Boolean.toString(plan.contains("SortMergeJoin")))
                    .attribute("plan.has_shj",Boolean.toString(plan.contains("ShuffledHashJoin")))
                    .attribute("plan.has_cartesian",Boolean.toString(plan.contains("CartesianProduct")))
                    .attribute("plan.has_bnlj",Boolean.toString(plan.contains("BroadcastNestedLoopJoin")))
                    .attribute("plan.partition_filters_empty",Boolean.toString(plan.contains("PartitionFilters: []")))
                    .attribute("aqe.changed_plan",Boolean.toString(plan.contains("AdaptiveSparkPlan")||plan.contains("AQEShuffleRead")))
                    .attribute("plan.codegen_gap",Boolean.toString(lower.contains("udf")&&!plan.matches("(?s).*\\*\\([0-9]+\\).*")));
        }
        int failed=0;int retries=0;
        for(StageAnalysis stage:sql.stages()){failed+=stage.failedTaskAttempts();retries+=stage.extraTaskAttempts();}
        sqlBuilder.number("failed_tasks",failed).number("stage_retries",retries);
        LimitProbeSummary limit = sql.limitProbe();
        if (limit != null) {
            sqlBuilder.capability(Capability.LIMIT_PROBE_METRICS)
                    .number("limit.rounds", limit.rounds())
                    .number("limit.scanned_partitions", limit.scannedPartitions())
                    .number("limit.initial_partitions", limit.initialPartitions())
                    .number("limit.scale_up_factor", limit.scaleUpFactor())
                    .number("limit.executor_cores", limit.executorCores())
                    .number("limit.second_round_partitions", limit.secondRoundPartitions())
                    .attribute("limit.initial_source", limit.initialSource());
        }
        out.add(sqlBuilder.build());

        for(StageAnalysis stage:sql.stages()){
            MetricsContext.Builder b=MetricsContext.builder(RuleScope.STAGE).executionId(sql.executionId()).stageId(stage.stageId())
                    .capability(Capability.BASE_TASK_METRICS).partial(incomplete)
                    .number("num_tasks",stage.numTasks()).number("alive_cores",Math.max(1,cores))
                    .number("task_duration.max_ms",stage.maxTaskMs()).number("task_duration.p50_ms",stage.medianTaskMs())
                    .number("task_duration.sum_ms",stage.totalTaskTimeMs()).number("runtime.sum_ms",stage.totalTaskTimeMs())
                    .number("duration.skew_ratio",stage.skewRatio()).number("shuffle_read.skew_ratio",stage.shuffleSkewRatio())
                    .number("shuffle_read.sum_bytes",stage.shuffleReadBytes()).number("shuffle_read.p50_bytes",stage.shuffleReadMedianBytes())
                    .number("shuffle_read.max_bytes",stage.shuffleReadMaxBytes()).number("shuffle_write.sum_bytes",stage.shuffleWriteBytes())
                    .number("spill.memory_sum_bytes",stage.memorySpillBytes()).number("spill.disk_sum_bytes",stage.diskSpillBytes())
                    .number("gc.ratio",stage.gcRatio()).number("gc.sum_ms",stage.gcRatio()*stage.totalTaskTimeMs())
                    .number("input.sum_bytes",stage.inputBytes()).number("input.p50_bytes",stage.medianInputBytesPerTask())
                    .number("fetch_wait.sum_ms",stage.shuffleFetchWaitMs()).number("fetch_wait.ratio",ratio(stage.shuffleFetchWaitMs(),stage.totalTaskTimeMs()))
                    .number("deserialize.p95_ms",stage.deserializeP95Ms()).number("deserialize.sum_ms",stage.deserializeSumMs())
                    .number("output.sum_bytes",stage.outputBytes()).number("failed_tasks",stage.failedTaskAttempts())
                    .number("extra_attempts",stage.extraTaskAttempts()).number("impact_wall_ms",stage.wallClockMs());
            out.add(b.build());
        }
        if(incomplete){
            out.add(MetricsContext.builder(RuleScope.DATA_QUALITY).capability(Capability.DATA_QUALITY)
                    .attribute("inprogress","true").number("partial_stage_count",sql.stages().size()).partial(true).build());
        }
        return Java8Collections.listCopy(out);
    }

    private static double ratio(long a,long b){return b<=0L?0.0:(double)a/(double)b;}
    private static int parse(String value,int fallback){if(value==null)return fallback;try{return Integer.parseInt(value.trim());}catch(NumberFormatException ex){return fallback;}}
}
