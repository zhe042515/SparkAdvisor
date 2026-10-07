package io.sparkadvisor.analyzer.v2;

import io.sparkadvisor.core.util.Java8Collections;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.nio.file.Path;
import java.nio.file.Files;
import java.io.BufferedReader;
import java.io.IOException;

/** Externalizable threshold registry whose keys match docs/rules.md section 7. */
public final class RuleThresholdsV2 {
    private final Map<String,Double> values;

    public RuleThresholdsV2(Map<String,Double> values){this.values=Java8Collections.mapCopy(values);}
    public double get(String key){Double value=values.get(key);if(value==null)throw new IllegalArgumentException("Missing rule threshold: "+key);return value.doubleValue();}
    public boolean contains(String key){return values.containsKey(key);}
    public Set<String> keys(){return values.keySet();}
    public RuleThresholdsV2 with(String key,double value){Map<String,Double> copy=new LinkedHashMap<String,Double>(values);copy.put(key,Double.valueOf(value));return new RuleThresholdsV2(copy);}

    public static RuleThresholdsV2 from(Map<String,String> config){
        RuleThresholdsV2 result=defaults();
        if(config==null)return result;
        Map<String,Double> copy=new LinkedHashMap<String,Double>(result.values);
        for(Map.Entry<String,String> entry:config.entrySet()){
            String prefix="spark.sparkadvisor.threshold.";
            if(entry.getKey().startsWith(prefix)){
                try{copy.put(entry.getKey().substring(prefix.length()),Double.valueOf(entry.getValue()));}
                catch(NumberFormatException ex){throw new IllegalArgumentException("Invalid threshold "+entry.getKey()+"="+entry.getValue(),ex);}
            }
        }
        return new RuleThresholdsV2(copy);
    }

    /** Load the inline-map threshold subset used by docs/rules.md without a YAML dependency. */
    public static RuleThresholdsV2 fromYaml(Path path) throws IOException {
        Map<String,Double> parsed=new LinkedHashMap<String,Double>();
        BufferedReader reader=Files.newBufferedReader(path,java.nio.charset.StandardCharsets.UTF_8);
        boolean inThresholds=false;String group=null;StringBuilder body=new StringBuilder();
        try{
            String raw;
            while((raw=reader.readLine())!=null){
                String line=stripComment(raw);String trimmed=line.trim();
                if(!inThresholds){if("thresholds:".equals(trimmed))inThresholds=true;continue;}
                if(group!=null){body.append(' ').append(trimmed);if(trimmed.contains("}")){parseInline(parsed,group,body.toString());group=null;body.setLength(0);}continue;}
                if(trimmed.isEmpty())continue;
                if(!Character.isWhitespace(line.charAt(0)))break;
                int colon=trimmed.indexOf(':');if(colon<=0)continue;
                String candidate=trimmed.substring(0,colon).trim();String remainder=trimmed.substring(colon+1).trim();
                if(!remainder.startsWith("{"))continue;
                group=candidate;body.append(remainder);
                if(remainder.contains("}")){parseInline(parsed,group,body.toString());group=null;body.setLength(0);}
            }
        }finally{reader.close();}
        Set<String> missing=new java.util.LinkedHashSet<String>(defaults().keys());missing.removeAll(parsed.keySet());
        if(!missing.isEmpty())throw new IllegalArgumentException("Missing thresholds in "+path+": "+missing);
        return new RuleThresholdsV2(parsed);
    }

    private static void parseInline(Map<String,Double> target,String group,String value){
        int open=value.indexOf('{'),close=value.lastIndexOf('}');if(open<0||close<=open)return;
        String[] entries=value.substring(open+1,close).split(",");
        for(String entry:entries){int colon=entry.indexOf(':');if(colon<=0)continue;String key=entry.substring(0,colon).trim();String raw=entry.substring(colon+1).trim();double numeric;if("true".equalsIgnoreCase(raw))numeric=1.0;else if("false".equalsIgnoreCase(raw))numeric=0.0;else numeric=Double.parseDouble(raw);target.put(group+"."+key,Double.valueOf(numeric));}
    }
    private static String stripComment(String line){int hash=line.indexOf('#');return hash<0?line:line.substring(0,hash);}

    public static RuleThresholdsV2 defaults(){
        Map<String,Double> v=new LinkedHashMap<String,Double>();
        put(v,"limit.max_rounds",2);
        put(v,"skew.min_tasks",20);put(v,"skew.abs_ms",120000);put(v,"skew.ratio",5);put(v,"skew.bytes_ratio",8);put(v,"skew.bytes_abs",1073741824L);
        put(v,"partitions.many_tasks",2000);put(v,"partitions.tiny_ms",2000);put(v,"partitions.overhead_ratio",0.3);put(v,"partitions.huge_bytes",536870912L);
        put(v,"small_files.scan_files",1000);put(v,"small_files.scan_avg_bytes",8388608L);put(v,"small_files.out_files",500);put(v,"small_files.out_avg_bytes",16777216L);put(v,"small_files.prune_files",5000);
        put(v,"spill.abs_bytes",10737418240L);put(v,"spill.ratio",0.2);
        put(v,"gc.warn",0.10);put(v,"gc.crit",0.20);put(v,"gc.min_runtime_ms",600000);
        put(v,"memory.peak_risk_ratio",0.9);put(v,"memory.peak_waste_ratio",0.5);put(v,"memory.sizing_factor",1.25);
        put(v,"cpu.bound_ratio",0.7);put(v,"cpu.io_ratio",0.3);
        put(v,"fetch_wait.ratio",0.15);put(v,"fetch_wait.min_shuffle_bytes",1073741824L);
        put(v,"serialization.deser_p95_ms",5000);put(v,"serialization.result_ser_ratio",0.1);put(v,"serialization.result_sum_bytes",1073741824L);put(v,"serialization.result_task_bytes",268435456L);
        put(v,"shuffle_write.min_bytes",10737418240L);put(v,"shuffle_write.throughput_bytes_per_s",52428800L);put(v,"shuffle_write.task_ratio",0.3);
        put(v,"locality.bad_ratio",0.5);put(v,"locality.min_tasks",200);
        put(v,"scheduler_delay.p50_ms",1000);put(v,"queue.wait_ratio",0.4);put(v,"queue.busy_ratio",0.85);put(v,"driver_gap.ratio",0.3);put(v,"driver_gap.min_duration_ms",60000);
        put(v,"fragmentation.min_jobs",50);put(v,"fragmentation.job_median_ms",3000);put(v,"fragmentation.gap_ratio",0.2);
        put(v,"broadcast.opportunity_bytes",67108864L);put(v,"broadcast.risk_bytes",536870912L);put(v,"dangerous_join.bnlj_rows",100000000L);put(v,"dangerous_join.critical_path_ratio",0.3);put(v,"row_amp.ratio",10);put(v,"row_amp.min_rows",100000000L);put(v,"codegen.cpu_ratio",0.7);put(v,"codegen.critical_path_ratio",0.2);put(v,"speculation.min_tasks",50);
        put(v,"regression.ratio",1.5);put(v,"regression.abs_ms",300000);put(v,"regression.min_samples",3);put(v,"regression.baseline_rounds",14);
        put(v,"host.score",1.5);put(v,"host.min_tasks",50);put(v,"host.min_stages",3);put(v,"capacity.idle_window_min",30);put(v,"capacity.idle_ratio",0.2);put(v,"capacity.overload_ratio",0.95);put(v,"monopoly.window_min",10);put(v,"monopoly.core_ratio",0.6);put(v,"monopoly.min_concurrent",3);put(v,"systemic.norm_median",1.4);put(v,"systemic.min_concurrent",3);
        put(v,"io_hotspot.host_throughput_ratio",0.4);put(v,"io_hotspot.min_tasks",100);put(v,"io_hotspot.spill_share",0.3);put(v,"net_matrix.min_fetchfail",20);put(v,"net_matrix.src_row_ratio",0.5);put(v,"net_matrix.rack_block_ratio",0.5);put(v,"oom.waterline_ratio",0.9);put(v,"oom.concurrent_statements",3);
        put(v,"driver_health.gc_ratio",0.15);put(v,"driver_health.sched_delay_ms",1000);put(v,"driver_health.gap_ratio",0.25);put(v,"driver_health.big_result_n",10);put(v,"storm.fail_ratio",0.1);put(v,"storm.fail_abs",200);put(v,"storm.retry_abs",20);put(v,"restart_win.tail_min",30);put(v,"queue_baseline.global_p50_ratio",1.3);put(v,"queue_baseline.regressed_fraction",0.3);put(v,"queue_baseline.baseline_rounds",14);put(v,"dq.partial_stage_warn",1);put(v,"dq.clock_skew_ms",5000);
        return new RuleThresholdsV2(v);
    }
    private static void put(Map<String,Double> map,String key,double value){map.put(key,Double.valueOf(value));}
}
