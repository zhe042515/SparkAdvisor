package io.sparkadvisor.core.analyze;

import io.sparkadvisor.core.util.Java8Collections;
import io.sparkadvisor.core.util.ValueObjects;

import java.util.*;

public final class SqlAnalysis {
    private final LimitProbeSummary limitProbe;
    private final long executionId,wallClockMs,criticalPathMs,idealMs; private final int maxConcurrentTasks; private final double avgConcurrentTasks; private final String statementId,description,physicalPlanText; private final double deviation,coreUtilization; private final List<StageAnalysis> stages;
    public SqlAnalysis(long executionId,String statementId,String description,String physicalPlanText,long wallClockMs,long criticalPathMs,long idealMs,double deviation,double coreUtilization,List<StageAnalysis> stages){this(executionId,statementId,description,physicalPlanText,wallClockMs,criticalPathMs,idealMs,deviation,coreUtilization,0,0.0,stages);}
    public SqlAnalysis(long executionId,String statementId,String description,String physicalPlanText,long wallClockMs,long criticalPathMs,long idealMs,double deviation,double coreUtilization,int maxConcurrentTasks,double avgConcurrentTasks,List<StageAnalysis> stages){this(executionId,statementId,description,physicalPlanText,wallClockMs,criticalPathMs,idealMs,deviation,coreUtilization,maxConcurrentTasks,avgConcurrentTasks,stages,null);}
    public SqlAnalysis(long executionId,String statementId,String description,String physicalPlanText,long wallClockMs,long criticalPathMs,long idealMs,double deviation,double coreUtilization,int maxConcurrentTasks,double avgConcurrentTasks,List<StageAnalysis> stages,LimitProbeSummary limitProbe){this.limitProbe=limitProbe;this.executionId=executionId;this.statementId=statementId;this.description=description;this.physicalPlanText=physicalPlanText;this.wallClockMs=wallClockMs;this.criticalPathMs=criticalPathMs;this.idealMs=idealMs;this.deviation=deviation;this.coreUtilization=coreUtilization;this.maxConcurrentTasks=maxConcurrentTasks;this.avgConcurrentTasks=avgConcurrentTasks;this.stages=Java8Collections.listCopy(stages);}
    public LimitProbeSummary limitProbe(){return limitProbe;}
    public long executionId(){return executionId;} public String statementId(){return statementId;} public String description(){return description;} public String physicalPlanText(){return physicalPlanText;} public long wallClockMs(){return wallClockMs;} public long criticalPathMs(){return criticalPathMs;} public long idealMs(){return idealMs;} public double deviation(){return deviation;} public double coreUtilization(){return coreUtilization;} public int maxConcurrentTasks(){return maxConcurrentTasks;} public double avgConcurrentTasks(){return avgConcurrentTasks;} public List<StageAnalysis> stages(){return stages;}
    public List<StageAnalysis> stagesByDurationDesc(){List<StageAnalysis> out=new ArrayList<StageAnalysis>(stages); Collections.sort(out,new Comparator<StageAnalysis>(){public int compare(StageAnalysis a,StageAnalysis b){return Long.compare(b.wallClockMs(),a.wallClockMs());}}); return Java8Collections.listCopy(out);}
    @Override public boolean equals(Object o){return ValueObjects.equalFields(this,o);}
    @Override public int hashCode(){return ValueObjects.hashFields(this);}
    @Override public String toString(){return ValueObjects.toString(this);}
}
