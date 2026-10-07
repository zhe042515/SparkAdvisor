package io.sparkadvisor.ui.live;

import io.sparkadvisor.core.eventlog.EventRetentionPolicy;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.ui.SparkUI;

import java.util.Collections;
import java.util.Map;
import java.util.logging.Logger;

import scala.Option;

/**
 * Driver component that attaches the SparkAdvisor tab to a live Spark UI.
 */
public final class SparkAdvisorDriverPlugin implements DriverPlugin {

    public static final String LIVE_ENABLED = "spark.sparkadvisor.live.enabled";
    public static final String COLLECT_TASK_INTERVALS =
            "spark.sparkadvisor.live.collectTaskIntervals";
    public static final String RETAINED_SQL_EXECUTIONS =
            "spark.sparkadvisor.live.retainedSqlExecutions";
    public static final String RETAINED_JOBS = "spark.sparkadvisor.live.retainedJobs";
    public static final String RETAINED_STAGES = "spark.sparkadvisor.live.retainedStages";
    public static final String RETAINED_TASK_INTERVALS =
            "spark.sparkadvisor.live.retainedTaskIntervals";
    public static final String RETAINED_EXECUTOR_EVENTS =
            "spark.sparkadvisor.live.retainedExecutorEvents";
    public static final String MAX_DESCRIPTION_CHARS =
            "spark.sparkadvisor.live.maxDescriptionChars";
    public static final String MAX_PLAN_CHARS = "spark.sparkadvisor.live.maxPlanChars";

    private static final Logger LOG = Logger.getLogger(SparkAdvisorDriverPlugin.class.getName());

    private SparkContext sparkContext;
    private LiveApplicationStore listener;

    @Override
    public Map<String, String> init(SparkContext sc, PluginContext pluginContext) {
        SparkConf conf = pluginContext.conf();
        if (!conf.getBoolean(LIVE_ENABLED, false)) {
            LOG.info("SparkAdvisor live UI plugin is loaded but disabled by " + LIVE_ENABLED);
            return Collections.emptyMap();
        }

        this.sparkContext = sc;
        boolean collectTaskIntervals = conf.getBoolean(COLLECT_TASK_INTERVALS, false);
        EventRetentionPolicy defaults = EventRetentionPolicy.liveDefaults();
        EventRetentionPolicy retention = new EventRetentionPolicy(
                nonNegative(conf, RETAINED_SQL_EXECUTIONS, defaults.completedSqlExecutions()),
                nonNegative(conf, RETAINED_JOBS, defaults.completedJobs()),
                nonNegative(conf, RETAINED_STAGES, defaults.completedStages()),
                nonNegative(conf, RETAINED_TASK_INTERVALS, defaults.taskIntervals()),
                nonNegative(conf, RETAINED_EXECUTOR_EVENTS, defaults.executorEvents()),
                defaults.pendingStatementIds(),
                nonNegative(conf, MAX_DESCRIPTION_CHARS, defaults.descriptionChars()),
                nonNegative(conf, MAX_PLAN_CHARS, defaults.physicalPlanChars()),
                false);
        this.listener = new LiveApplicationStore(collectTaskIntervals, retention);
        // VERIFY@3.5.1: DriverPlugin.init runs before listenerBus.start(), and
        // SparkContext.addSparkListener registers with the shared queue.
        sc.addSparkListener(listener);

        try {
            Option<SparkUI> uiOption = sc.ui(); // VERIFY@3.5.1: SparkContext.ui(): Option[SparkUI]
            if (uiOption.isDefined()) {
                SparkUI ui = uiOption.get();
                ui.attachTab(new LiveSparkAdvisorTab(ui, listener));
                LOG.info("SparkAdvisor live tab attached with bounded retention: sql="
                        + retention.completedSqlExecutions() + ", jobs=" + retention.completedJobs()
                        + ", stages=" + retention.completedStages() + ", taskIntervals="
                        + (collectTaskIntervals ? retention.taskIntervals() : 0));
            } else {
                LOG.warning("SparkAdvisor live UI is enabled but Spark UI is disabled; "
                        + "no tab will be attached");
            }
        } catch (Throwable t) {
            LOG.warning("Failed to attach SparkAdvisor live tab: " + t);
        }
        return Collections.emptyMap();
    }

    @Override
    public void registerMetrics(String appId, PluginContext pluginContext) {
        LOG.info("SparkAdvisor live UI initialized for app " + appId);
    }

    @Override
    public void shutdown() {
        if (sparkContext != null && listener != null) {
            try {
                sparkContext.removeSparkListener(listener);
            } catch (Throwable t) {
                LOG.fine("Ignoring SparkAdvisor listener removal failure during shutdown: " + t);
            }
        }
    }

    private static int nonNegative(SparkConf conf, String key, int defaultValue) {
        return Math.max(0, conf.getInt(key, defaultValue));
    }
}
