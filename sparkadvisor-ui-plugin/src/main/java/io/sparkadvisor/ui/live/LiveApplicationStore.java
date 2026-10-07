package io.sparkadvisor.ui.live;

import io.sparkadvisor.core.eventlog.SparkEventCollector;
import io.sparkadvisor.core.eventlog.EventRetentionPolicy;
import io.sparkadvisor.core.model.ApplicationModel;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerExecutorAdded;
import org.apache.spark.scheduler.SparkListenerExecutorRemoved;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;

/**
 * Thread-safe live wrapper around {@link SparkEventCollector}.
 *
 * <p>Spark listener callbacks arrive on Spark's listener threads, while the UI reads snapshots
 * on Jetty request threads. The underlying collector is deliberately single-threaded for replay,
 * so this wrapper serializes mutations and snapshot materialization.
 */
public final class LiveApplicationStore extends SparkListener {

    private final SparkEventCollector collector;
    private final boolean collectTaskIntervals;

    public LiveApplicationStore(boolean collectTaskIntervals) {
        this(collectTaskIntervals, EventRetentionPolicy.liveDefaults());
    }

    public LiveApplicationStore(boolean collectTaskIntervals, EventRetentionPolicy retention) {
        this.collectTaskIntervals = collectTaskIntervals;
        this.collector = new SparkEventCollector(collectTaskIntervals, retention);
    }

    public synchronized ApplicationModel snapshot() {
        return collector.build();
    }

    public boolean collectTaskIntervals() {
        return collectTaskIntervals;
    }

    public synchronized String retentionSummary() {
        return collector.retentionSummary();
    }

    @Override
    public synchronized void onApplicationStart(SparkListenerApplicationStart e) {
        collector.onApplicationStart(e);
    }

    @Override
    public synchronized void onApplicationEnd(SparkListenerApplicationEnd e) {
        collector.onApplicationEnd(e);
    }

    @Override
    public synchronized void onEnvironmentUpdate(SparkListenerEnvironmentUpdate e) {
        collector.onEnvironmentUpdate(e);
    }

    @Override
    public synchronized void onExecutorAdded(SparkListenerExecutorAdded e) {
        collector.onExecutorAdded(e);
    }

    @Override
    public synchronized void onExecutorRemoved(SparkListenerExecutorRemoved e) {
        collector.onExecutorRemoved(e);
    }

    @Override
    public synchronized void onJobStart(SparkListenerJobStart e) {
        collector.onJobStart(e);
    }

    @Override
    public synchronized void onJobEnd(SparkListenerJobEnd e) {
        collector.onJobEnd(e);
    }

    @Override
    public synchronized void onStageSubmitted(SparkListenerStageSubmitted e) {
        collector.onStageSubmitted(e);
    }

    @Override
    public synchronized void onStageCompleted(SparkListenerStageCompleted e) {
        collector.onStageCompleted(e);
    }

    @Override
    public synchronized void onTaskEnd(SparkListenerTaskEnd e) {
        collector.onTaskEnd(e);
    }

    @Override
    public synchronized void onOtherEvent(SparkListenerEvent event) {
        collector.onOtherEvent(event);
    }
}
