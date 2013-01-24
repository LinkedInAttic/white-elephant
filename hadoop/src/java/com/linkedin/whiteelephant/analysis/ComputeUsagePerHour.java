/*
 * Copyright 2012 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.whiteelephant.analysis;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.linkedin.whiteelephant.analysis.AttemptStatsKey;
import com.linkedin.whiteelephant.analysis.AttemptStatsValue;
import com.linkedin.whiteelephant.analysis.TaskStatus;
import com.linkedin.whiteelephant.analysis.TaskType;
import com.linkedin.whiteelephant.parsing.LogData;
import com.linkedin.whiteelephant.mapreduce.lib.job.StagedOutputJob;
import com.linkedin.whiteelephant.mapreduce.lib.job.StagedOutputJobExecutor;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ComputeUsagePerHour
{
  private static final String REDUCE_SHUFFLE_BYTES = "REDUCE_SHUFFLE_BYTES";
  private static final String CPU_MILLISECONDS = "CPU_MILLISECONDS";
  private static final String SPILLED_RECORDS = "SPILLED_RECORDS";
  
  private final Logger _log;
  private final FileSystem _fs;
  private final Properties _props;
  private final String _name;
  
  private final static TimeZone timeZone = TimeZone.getTimeZone("GMT");
  
  private final String _jobsOutputPathRoot;
  private final String _usageOutputPathRoot;
  private final boolean _incremental;
  private final int _numDaysForced;
  
  public ComputeUsagePerHour(String name, Properties props) throws IOException
  {
    _log = Logger.getLogger(name);
    _name = name;
    _props = props;
    _fs = FileSystem.get(StagedOutputJob.createConfigurationFromProps(_props));
    
    if (_props.get("jobs.output.path") == null) {
      throw new IllegalArgumentException("joined.output.path is not specified.");
    }
    
    if (_props.get("usage.output.path") == null) {
      throw new IllegalArgumentException("usage.output.path is not specified.");
    }
    
    if (_props.get("num.days.forced") == null) {
      throw new IllegalArgumentException("num.days.forced is not specified");
    }
    
    _usageOutputPathRoot = (String)_props.get("usage.output.path");
    _jobsOutputPathRoot = (String)_props.get("jobs.output.path");
    _incremental = Boolean.parseBoolean((String)_props.get("incremental"));
    _numDaysForced = Integer.parseInt((String)_props.get("num.days.forced"));
  }
  
  public void execute(StagedOutputJobExecutor executor) throws IOException, InterruptedException, ExecutionException
  {
    FileStatus[] clusterPaths = _fs.listStatus(new Path(_jobsOutputPathRoot));
    
    for (FileStatus clusterPath : clusterPaths)
    {
      String clusterName = clusterPath.getPath().getName();
      
      FileStatus[] yearPaths = _fs.listStatus(clusterPath.getPath());
      
      for (FileStatus yearPath : yearPaths)      
      {
        String year = yearPath.getPath().getName();
        
        System.out.println("Searching under " + yearPath.getPath());
        FileStatus[] dayPaths = _fs.listStatus(yearPath.getPath());
        for (FileStatus dayPath : dayPaths)
        {
          String day = dayPath.getPath().getName();
          
          String pattern = dayPath.getPath().toString() + "/*.avro";
          
          String outputPathForDay = String.format("%s/%s/%s/%s",_usageOutputPathRoot,clusterName,year,day);
          
          FileStatus[] inputFiles = _fs.globStatus(new Path(pattern));
          
          StringBuilder msg = new StringBuilder(pattern + " => " + inputFiles.length + " files");
          
          if (inputFiles.length > 0)
          {
            Calendar cal = Calendar.getInstance(timeZone);
            
            long nowMillis = cal.getTimeInMillis();
            
            cal.set(Integer.parseInt(year), Integer.parseInt(day.substring(0, 2)) - 1, Integer.parseInt(day.substring(2, 4)));
            
            long thenMillis = cal.getTimeInMillis();
            
            double elapsedDays = Math.max(0.0, ((double)(nowMillis - thenMillis))/(24*3600*1000));
            
            if (!_incremental || !_fs.exists(new Path(outputPathForDay)) || elapsedDays < _numDaysForced)
            {
              long totalLength = 0;
              for (FileStatus stat : inputFiles)
              {         
                totalLength += stat.getLen();
              }
              
              msg.append(String.format(", %s MB",totalLength/1024/1024));            
              System.out.println(msg);
              
              // one reducer per 1 GB
              int numReducers = (int)Math.ceil(((double)totalLength)/1024/1024/1024);
              
              submitJob(executor, pattern, outputPathForDay, clusterName, year, day, numReducers);
            }
            else if (_incremental && _fs.exists(new Path(outputPathForDay)))
            {
              msg.append(" (skipping)");
              System.out.println(msg);
            }
          }
        }
        
        executor.waitForCompletion();
      }
    }
  }
  
  private void submitJob(StagedOutputJobExecutor executor, String inputPattern, String output, String clusterName, String year, String day, int numReducers)
  {
    List<String> inputPaths = new ArrayList<String>();
    
    inputPaths.add(inputPattern);
    
    final StagedOutputJob job = StagedOutputJob.createStagedJob(
      _props,
      _name + "-" + "usage-per-hour-" + clusterName + "-" + year + "-" + day,
      inputPaths,
      "/tmp" + output,
      output,
      _log);
    
    final Configuration conf = job.getConfiguration();
    
    conf.set("cluster.name", clusterName);
                
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(BytesWritable.class);
    
    job.setInputFormatClass(AvroKeyValueInputFormat.class);
    job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
    
    AvroJob.setInputKeySchema(job, Schema.create(Type.STRING));
    AvroJob.setInputValueSchema(job, LogData.SCHEMA$);
    
    AvroJob.setMapOutputKeySchema(job, AttemptStatsKey.SCHEMA$);
    AvroJob.setMapOutputValueSchema(job, AttemptStatsValue.SCHEMA$);
    
    AvroJob.setOutputKeySchema(job, AttemptStatsKey.SCHEMA$);
    AvroJob.setOutputValueSchema(job, AttemptStatsValue.SCHEMA$);
    
    job.setNumReduceTasks(numReducers);
    
    job.setMapperClass(ComputeUsagePerHour.TheMapper.class);
    job.setReducerClass(ComputeUsagePerHour.TheReducer.class);
    
    executor.submit(job);
  }
  
  public static class TheMapper extends Mapper<AvroKey<String>, AvroValue<LogData>, AvroWrapper<AttemptStatsKey>, AvroWrapper<AttemptStatsValue>>
  { 
    private String clusterName;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      System.out.println("Setting up reducer");
      Configuration conf = context.getConfiguration();
      clusterName = conf.get("cluster.name");
      System.out.println("Got cluster " + clusterName);
      super.setup(context);
    }
    
    @Override
    protected void map(AvroKey<String> key, AvroValue<LogData> value, Context context) throws java.io.IOException, java.lang.InterruptedException
    { 
      LogData data = value.datum();
      
      if (data.getEntry() != null && data.getEntry() instanceof com.linkedin.whiteelephant.parsing.Job)
      {
        com.linkedin.whiteelephant.parsing.Job job = (com.linkedin.whiteelephant.parsing.Job)data.getEntry();
        for (com.linkedin.whiteelephant.parsing.Task task : job.getTasks())
        {
          for (com.linkedin.whiteelephant.parsing.Attempt attempt : task.getAttempts())
          {
            if (attempt.getTaskStatus() == null)
            {
              System.out.println("Status null for job " + attempt.getJobId() + " attempt " + attempt.getTaskAttemptId());            
              context.getCounter("Job Analysis", "Status null").increment(1);            
              continue;
            }
            else
            {
              context.getCounter("Job Analysis", "Status " + attempt.getTaskStatus()).increment(1);
            }
            
            if (attempt.getStartTime() == null || attempt.getFinishTime() == null)
            {
              System.out.println("Missing either startTime or finishTime");
              context.getCounter("Job Analysis", "Missing start or finish").increment(1);
              continue;
            }
            
            if (!(attempt.getStartTime() > 0 && attempt.getFinishTime() > 0))
            {
              System.out.println("Either startTime or finishTime is not positive");
              context.getCounter("Job Analysis", "Not positive start or finish").increment(1);
              continue;
            }

            AttemptStatsKey statsKey = new AttemptStatsKey();
            statsKey.setCluster(clusterName); 
            statsKey.setExcess(((com.linkedin.whiteelephant.parsing.DerivedAttemptData)attempt.getDerived()).getExcess());
            statsKey.setStatus(TaskStatus.valueOf(attempt.getTaskStatus().toString()));
            statsKey.setType(TaskType.valueOf(attempt.getType().toString().toUpperCase()));
            statsKey.setUser(job.getUser());
            
            writeStats(statsKey, attempt, context);
          }
        }
      }        
    }
    
    private void writeStats(AttemptStatsKey key, com.linkedin.whiteelephant.parsing.Attempt attempt, Context context) throws IOException, InterruptedException
    {      
      Long start = attempt.getStartTime();
      Long end = attempt.getFinishTime();
      
      if (end < start)
      {
        throw new RuntimeException(String.format("finishTime %s is less than startTime %s", end, start));
      }
      
      TimeUnit unit = TimeUnit.HOURS;
      Long currentTime = start;
      
      key.setUnit(com.linkedin.whiteelephant.analysis.TimeUnit.HOURS);
      
      while (currentTime < end)
      {        
        Calendar currentUnitStart = getCalendarForTime(unit, currentTime);
        Calendar currentUnitEnd = getCalendarForTime(unit, currentTime);
        
        if (unit == TimeUnit.HOURS)
        {          
          currentUnitEnd.add(Calendar.HOUR, 1);
        }
        else
        {
          throw new RuntimeException("Unsupported time unit: " + unit);
        }
        
        long nextMillis = Math.min(currentUnitEnd.getTimeInMillis(),end);
        
        double percentOfTotal = (nextMillis - currentTime)/((double)(end - start));   
        
        AttemptStatsValue value = new AttemptStatsValue();
                
        value.setElapsedMinutes((nextMillis - currentTime)/1000.0/60.0);
        
        if (attempt.getCounters().get(CPU_MILLISECONDS) != null)
        {
          value.setCpuMinutes(percentOfTotal * attempt.getCounters().get(CPU_MILLISECONDS)/1000.0/60.0);
        }
        
        if (attempt.getCounters().get(SPILLED_RECORDS) != null)
        {
          value.setSpilledRecords((long)(percentOfTotal * attempt.getCounters().get(SPILLED_RECORDS)));
        }
        
        if (attempt.getCounters().get(REDUCE_SHUFFLE_BYTES) != null)
        {
          value.setReduceShuffleBytes(attempt.getCounters().get(REDUCE_SHUFFLE_BYTES));
        }
                
        key.setTime(currentUnitStart.getTimeInMillis());
        
        if ((key.getTime() + unit.toMillis(1)) >= start && start >= key.getTime())
        {
          value.setStarted(1);
        }
        
        if ((key.getTime() + unit.toMillis(1)) >= end && end >= key.getTime())
        {
          value.setFinished(1);
        }
        
        currentTime = nextMillis;
        
        context.write(new AvroKey<AttemptStatsKey>(key), new AvroValue<AttemptStatsValue>(value));
      }
    }
        
    private static Calendar getCalendarForTime(TimeUnit unit, Long time)
    { 
      Calendar cal = Calendar.getInstance(timeZone);
      cal.setTimeInMillis(time);        

      if (unit == TimeUnit.HOURS)
      {
        int dstOffset = cal.get(Calendar.DST_OFFSET);
        
        // zero these out so we can advance to the next boundary simply by adding an hour
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        
        // reset the DST offset, since setting above fields to 0 for some reason alters the value
        cal.set(Calendar.DST_OFFSET, dstOffset);
      }
      else if (unit == TimeUnit.DAYS)
      {
        int dstOffset = cal.get(Calendar.DST_OFFSET);
        
        // zero these out so we can advance to the next boundary simply by adding a day
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        
        // reset the DST offset, since setting above fields to 0 for some reason alters the value
        cal.set(Calendar.DST_OFFSET, dstOffset);
      }
      else
      {
        throw new RuntimeException("Unsupported time unit: " + unit);
      }
      
      return cal;
    }
  }
  
  public static class TheReducer extends Reducer<AvroKey<AttemptStatsKey>, AvroValue<AttemptStatsValue>, AvroWrapper<AttemptStatsKey>, AvroWrapper<AttemptStatsValue>> 
  {
    private String clusterName;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      System.out.println("Setting up reducer");
      Configuration conf = context.getConfiguration();
      clusterName = conf.get("cluster.name");
      System.out.println("Got cluster " + clusterName);
      super.setup(context);
    }
    
    @Override
    protected void reduce(AvroKey<AttemptStatsKey> key, Iterable<AvroValue<AttemptStatsValue>> values, final Context context) throws IOException, InterruptedException 
    { 
      AttemptStatsValue merged = new AttemptStatsValue();
            
      merged.setElapsedMinutes(0.0);
      merged.setStarted(0);
      merged.setFinished(0);
      
      for (AvroValue<AttemptStatsValue> valueWrapped : values)
      {
        AttemptStatsValue value = valueWrapped.datum();
        merged.setElapsedMinutes(value.getElapsedMinutes() + merged.getElapsedMinutes());
        merged.setStarted(value.getStarted() + merged.getStarted());
        merged.setFinished(value.getFinished() + merged.getFinished());
        
        if (value.getCpuMinutes() != null)
        {
          if (merged.getCpuMinutes() == null)
          {
            merged.setCpuMinutes(value.getCpuMinutes());
          }
          else
          {
            merged.setCpuMinutes(merged.getCpuMinutes() + value.getCpuMinutes());
          }
        }
        
        if (value.getSpilledRecords() != null)
        {
          if (merged.getSpilledRecords() == null)
          {
            merged.setSpilledRecords(value.getSpilledRecords());
          }
          else
          {
            merged.setSpilledRecords(merged.getSpilledRecords() + value.getSpilledRecords());
          }
        }
        
        if (value.getReduceShuffleBytes() != null)
        {
          if (merged.getReduceShuffleBytes() == null)
          {
            merged.setReduceShuffleBytes(value.getReduceShuffleBytes());
          }
          else 
          {
            merged.setReduceShuffleBytes(merged.getReduceShuffleBytes() + value.getReduceShuffleBytes());
          }
        }
      }
      
      context.write(key, new AvroWrapper<AttemptStatsValue>(merged));
    }
  }
}
