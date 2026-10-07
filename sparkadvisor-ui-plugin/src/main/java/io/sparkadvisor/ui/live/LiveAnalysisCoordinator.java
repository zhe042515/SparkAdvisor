package io.sparkadvisor.ui.live;

import io.sparkadvisor.advisor.AdvisorFactory;
import io.sparkadvisor.advisor.api.TuningAdvisor;
import io.sparkadvisor.core.locate.SqlLocator;
import io.sparkadvisor.core.model.ApplicationModel;
import io.sparkadvisor.core.model.SqlExecution;
import io.sparkadvisor.core.util.Strings;
import io.sparkadvisor.monitor.QueueAnalysisContext;
import io.sparkadvisor.monitor.QueueAnalyzer;
import io.sparkadvisor.monitor.aggregate.QueueAnalysisResult;
import io.sparkadvisor.monitor.render.QueueHtmlWriter;
import io.sparkadvisor.report.html.HtmlReportWriter;
import io.sparkadvisor.report.i18n.ReportLanguage;
import io.sparkadvisor.report.model.AnalysisResult;
import io.sparkadvisor.report.model.AnalysisResultBuilder;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Renders reports from the live driver listener snapshot.
 */
public final class LiveAnalysisCoordinator {

    private final LiveApplicationStore store;
    private final HtmlReportWriter htmlWriter = new HtmlReportWriter();
    private final QueueHtmlWriter queueHtmlWriter = new QueueHtmlWriter();
    private final QueueAnalyzer queueAnalyzer = new QueueAnalyzer();

    public LiveAnalysisCoordinator(LiveApplicationStore store) {
        this.store = store;
    }

    public String stylesheet() {
        return htmlWriter.stylesheet();
    }

    public String queueStylesheet() {
        return queueHtmlWriter.stylesheet();
    }

    public String renderSqlBody(String statementId, ReportLanguage language) throws Exception {
        ApplicationModel model = store.snapshot();
        SqlExecution target = selectTarget(model, statementId);
        AnalysisResult result = new AnalysisResultBuilder(model, sourcePath(model)).build(target);
        if (result.targetSql() != null) {
            TuningAdvisor advisor = AdvisorFactory.forMode("rule", language);
            result = result.withAiAdvice(advisor.advise(result));
        }
        return htmlWriter.renderBody(result, language);
    }

    public String renderQueueBody(int topN, int samplePerStratum, long bucketMs,
                                  ReportLanguage language) throws Exception {
        ApplicationModel model = store.snapshot();
        String degradedReason = "";
        if (!store.collectTaskIntervals()) {
            degradedReason = "Live driver task-interval collection is disabled; contention and "
                    + "resource-occupancy metrics are omitted.";
        }
        String retentionSummary = store.retentionSummary();
        if (!retentionSummary.isEmpty()) {
            degradedReason = degradedReason.isEmpty()
                    ? retentionSummary
                    : degradedReason + " " + retentionSummary;
        }
        QueueAnalysisContext context = new QueueAnalysisContext(
                snapshotKey(model), true, degradedReason);
        QueueAnalysisResult result = queueAnalyzer.analyze(
                model, sourcePath(model), topN, samplePerStratum, bucketMs, context);
        return queueHtmlWriter.renderBody(result, language);
    }

    private SqlExecution selectTarget(ApplicationModel model, String statementId) {
        if (!Strings.isBlank(statementId)) {
            List<SqlExecution> matches = new SqlLocator(model).locate(statementId);
            return matches.isEmpty() ? null : matches.get(0);
        }
        Optional<SqlExecution> slowestCompleted = model.sqlExecutions().stream()
                .filter(s -> !s.incomplete() && s.wallClockMs() > 0L)
                .max(Comparator.comparingLong(SqlExecution::wallClockMs));
        if (slowestCompleted.isPresent()) {
            return slowestCompleted.get();
        }
        return model.sqlExecutions().stream()
                .max(Comparator.comparingLong(SqlExecution::startTime))
                .orElse(null);
    }

    private static String sourcePath(ApplicationModel model) {
        String appId = Strings.isBlank(model.appId()) ? "<pending-app-id>" : model.appId();
        return "live-driver:" + appId;
    }

    private static String snapshotKey(ApplicationModel model) {
        return sourcePath(model) + ":" + model.sqlExecutions().size() + ":" + model.jobs().size()
                + ":" + model.stages().size() + ":" + model.incomplete();
    }
}
