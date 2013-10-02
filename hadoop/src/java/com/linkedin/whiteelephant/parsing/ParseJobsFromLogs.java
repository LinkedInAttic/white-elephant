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

package com.linkedin.whiteelephant.parsing;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.linkedin.whiteelephant.parsing.Attempt;
import com.linkedin.whiteelephant.parsing.DerivedAttemptData;
import com.linkedin.whiteelephant.parsing.Job;
import com.linkedin.whiteelephant.parsing.LogData;
import com.linkedin.whiteelephant.parsing.Task;
import com.linkedin.whiteelephant.mapreduce.MyAvroMultipleOutputs;
import com.linkedin.whiteelephant.mapreduce.lib.input.CombinedTextInputFormat;
import com.linkedin.whiteelephant.mapreduce.lib.job.StagedOutputJob;
import com.linkedin.whiteelephant.mapreduce.lib.job.StagedOutputJobExecutor;
import com.linkedin.whiteelephant.parsing.LineParsing;
import com.linkedin.whiteelephant.util.JobStatsProcessing;

public class ParseJobsFromLogs
{  
  private static final String CPU_MILLISECONDS = "CPU_MILLISECONDS";
  
  private final Logger _log;
  private final Properties _props;
  private final FileSystem _fs;
  private final String _name;
  
  private final String _jobsOutputPathRoot;
  private final String _logsRoot;
  private final String _clusterNames;
  private final int _numDays;
  private final int _numDaysForced;
  private final boolean _incremental;
  
  public ParseJobsFromLogs(String name, Properties props) throws IOException
  {
    _log = Logger.getLogger(name);
    _name = name;
    _props = props;    
    
    Configuration conf = StagedOutputJob.createConfigurationFromProps(_props);
    
    System.out.println("fs.default.name: " + conf.get("fs.default.name"));
    
    _fs = FileSystem.get(conf);
    
    if (_props.get("cluster.names") == null) {
      throw new IllegalArgumentException("cluster.names is not specified.");
    }
    
    if (_props.get("jobs.output.path") == null) {
       throw new IllegalArgumentException("attempts.output.path is not specified.");
    }
    
    if (_props.get("num.days") == null) {
      throw new IllegalArgumentException("num.days is not specified");
    }
    
    if (_props.get("num.days.forced") == null) {
      throw new IllegalArgumentException("num.days.forced is not specified");
    }
    
    if (_props.get("incremental") == null) {
      throw new IllegalArgumentException("incremental is not specified.");
    }

    if (_props.get("logs.root") == null) {
      throw new IllegalArgumentException("logs.root is not specified.");
    }

    _jobsOutputPathRoot = (String)_props.get("jobs.output.path");
    _logsRoot = (String)_props.get("logs.root");
    _clusterNames = (String)_props.get("cluster.names");
    _numDays = Integer.parseInt((String)_props.get("num.days"));
    _numDaysForced = Integer.parseInt((String)_props.get("num.days.forced"));
    _incremental = Boolean.parseBoolean((String)_props.get("incremental"));
  }
  
  public void execute(StagedOutputJobExecutor executor) throws IOException, InterruptedException, ExecutionException
  {
    for (String clusterName : _clusterNames.split(","))
    {
      System.out.println("Processing cluster " + clusterName);
            
      List<JobStatsProcessing.ProcessingTask> processingTasks = JobStatsProcessing.getTasks(_fs, _logsRoot, clusterName, _jobsOutputPathRoot, "log", _incremental, _numDays, _numDaysForced);
      
      for (JobStatsProcessing.ProcessingTask task : processingTasks)
      {      
        List<String> inputPaths = new ArrayList<String>();
        inputPaths.add(task.inputPathFormat);
        
        String outputPath = task.outputPath;
        
        final StagedOutputJob job = StagedOutputJob.createStagedJob(
           _props,
           _name + "-parse-jobs-" + task.id,
           inputPaths,
           "/tmp" + outputPath,
           outputPath,
           _log);
        
        job.getConfiguration().set("jobs.output.path", _jobsOutputPathRoot);
        job.getConfiguration().set("logs.cluster.name", clusterName);
                
        // 1 reducer per 12 GB of input data
        long numReduceTasks = (int)Math.ceil(((double)task.totalLength) / 1024 / 1024 / 1024 / 12);
                
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(BytesWritable.class);
  
        job.setInputFormatClass(CombinedTextInputFormat.class);
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
  
        AvroJob.setOutputKeySchema(job, Schema.create(Type.STRING));
        AvroJob.setOutputValueSchema(job, LogData.SCHEMA$);
        
        job.setNumReduceTasks((int)numReduceTasks);
   
        job.setMapperClass(ParseJobsFromLogs.TheMapper.class);
        job.setReducerClass(ParseJobsFromLogs.TheReducer.class);
         
        AvroJob.setMapOutputKeySchema(job, Schema.create(Type.STRING));
        AvroJob.setMapOutputValueSchema(job, LogData.SCHEMA$);
        
        MyAvroMultipleOutputs.addNamedOutput(job, "logs", AvroKeyValueOutputFormat.class, Schema.create(Type.STRING), LogData.SCHEMA$);
        
        executor.submit(job);
      }
      
      executor.waitForCompletion();
    }
  }
  
  public static class TheMapper extends Mapper<LongWritable, Text, AvroWrapper<String>, AvroWrapper<LogData>> 
  {
    String _clusterName;
    
    @Override
    protected void setup(Context context)
    {
      _clusterName = context.getConfiguration().get("logs.cluster.name");
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
    {
      List<CharSequence> inputSplits = new ArrayList<CharSequence>();
      
      CombineFileSplit fileSplit = (CombineFileSplit) context.getInputSplit();
                  
      for (Path path : fileSplit.getPaths())
      {        
        inputSplits.add(path.toString());
      }
      
      String line = value.toString(); 
      
      Job job = null;
      Attempt attempt = null;
      Task task = null;
      
      job = LineParsing.tryParseJob(line);
      
      if (job == null)
      {      
        attempt = LineParsing.tryParseAttempt(line);
        
        if (attempt == null)
        {
          task = LineParsing.tryParseTask(line);
        }
      }
      
      LogData data = new LogData();
      String jobId = null;
      
      data.setCluster(_clusterName);
      
      try
      {
        if (job != null)
        {
          // log lines are sometimes truncated, so data may be missing - just ignore these
          if (job.getJobId() != null)
          {
            jobId = job.getJobId().toString();
            job.setTasks(new ArrayList<Task>());
            data.setEntry(job);
            data.setPath(findInputSplitForJob(jobId,inputSplits));
            context.write(new AvroKey<String>(jobId), new AvroValue<LogData>(data));
          }
        }
        else if (attempt != null)
        {
          // log lines are sometimes truncated, so data may be missing - just ignore these
          if (attempt.getJobId() != null && attempt.getTaskId() != null && attempt.getTaskAttemptId() != null && attempt.getType() != null)
          {
            jobId = attempt.getJobId().toString();
            data.setEntry(attempt);
            data.setPath(findInputSplitForJob(jobId,inputSplits));
            context.write(new AvroKey<String>(jobId), new AvroValue<LogData>(data));
          }
        }
        else if (task != null)
        {
          // log lines are sometimes truncated, so data may be missing - just ignore these
          if (task.getJobId() != null && task.getTaskId() != null && task.getType() != null)
          {
            jobId = task.getJobId().toString();
            task.setAttempts(new ArrayList<Attempt>());
            data.setEntry(task);
            data.setPath(findInputSplitForJob(jobId,inputSplits));
            context.write(new AvroKey<String>(jobId), new AvroValue<LogData>(data));
          }
        }
      }
      catch (Exception e)
      {
        System.out.println("Exception writing log data: " + e.toString());
        if (jobId != null)
        {
          System.out.println("jobId: " + jobId);
          CharSequence path = data.getPath();
          if (path != null)
          {
            System.out.println("input: " + path);
          }
        }
        throw new IOException(e);
      }
    }
    
    private CharSequence findInputSplitForJob(String jobId, List<CharSequence> inputSplits)
    {
      if (jobId != null)
      {
        for (CharSequence inputSplit : inputSplits)
        {
          if (inputSplit.toString().contains(jobId))
          {
            return inputSplit;
          }
        }
      }
      
      return null;
    }
  }
  
  public static class TheReducer extends Reducer<AvroKey<String>, AvroValue<LogData>, AvroWrapper<String>, AvroWrapper<LogData>> 
  {    
    private String jobOutputPath;
    
    @Override
    protected void setup(Context context)
    {
      jobOutputPath = context.getConfiguration().get("jobs.output.path");
      System.out.println("Job output path: " + jobOutputPath);
    }
    
    @Override
    protected void reduce(AvroKey<String> key, Iterable<AvroValue<LogData>> values, final Context context) throws IOException, InterruptedException 
    {      
      String jobId = key.datum();
      
      String inputPath = null;
      
      List<Job> jobEntries = new ArrayList<Job>();
      List<Attempt> attemptEntries = new ArrayList<Attempt>();
      List<Task> taskEntries = new ArrayList<Task>();
      for (AvroValue<LogData> value : values)
      {        
        inputPath = value.datum().getPath().toString();
        
        if (value.datum().getEntry() instanceof Job)
        {
          jobEntries.add(Job.newBuilder((Job)value.datum().getEntry()).build());
        }
        else if (value.datum().getEntry() instanceof Task)
        {
          taskEntries.add(Task.newBuilder((Task)value.datum().getEntry()).build());
        }
        else if (value.datum().getEntry() instanceof Attempt)
        {
          attemptEntries.add(Attempt.newBuilder((Attempt)value.datum().getEntry()).build());
        }
      }
      
      Job job = new Job();
      
      mergeJobEntries(job, jobEntries);
      mergeTaskEntries(job, taskEntries);
      mergeTaskAttemptEntries(job, attemptEntries);      
      
      LogData data = new LogData();
      data.setPath(inputPath);
      data.setEntry(job);
      
      try
      {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MMdd");     
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        
//        Date submitTime = new Date(job.getSubmitTime());
//        String baseOutputPath = String.format("%s/%s/part",cluster,dateFormat.format(submitTime));                        
//        amos.write("logs", new AvroWrapper<String>(jobId), new AvroWrapper<LogData>(data), baseOutputPath);
        
        context.write(new AvroWrapper<String>(jobId), new AvroWrapper<LogData>(data));
      }
      catch (Exception e)
      {
        System.out.println("Exception writing log data: " + e.toString());
        
        if (inputPath != null)
        {
          System.out.println("input: " + inputPath);
        }
        
        System.out.println("key: " + jobId);
        
        try
        {
          System.out.println("value:\n" + (new JSONObject(GenericData.get().toString(data)).toString(2)));
        }
        catch (Exception e1)
        {
          System.out.println("Error generating JSON value: " + e1.toString());
        }
        throw new IOException(e);
      }
    }
    
    public static void mergeJobEntries(Job parsedJob, List<Job> jobEntries/*, final Context context*/)
    {      
      for (Job jobLine : jobEntries)
      {        
        // ignore status unless it's the last one, status actually shows up multiple times as the job passes through various states
        if (jobLine.getJobStatus() != null && jobLine.getFinishTime() != null)
        {
          parsedJob.setJobStatus(jobLine.getJobStatus());
          parsedJob.setFinishTime(jobLine.getFinishTime());
        }
        
        if (jobLine.getUser() != null)
        {
          parsedJob.setUser(jobLine.getUser());
        }
        
        if (jobLine.getJobId() != null)
        {
          parsedJob.setJobId(jobLine.getJobId());
        }
        
        if (jobLine.getUser() != null)
        {
          parsedJob.setUser(jobLine.getUser());
        }
        
        if (jobLine.getJobName() != null)
        {
          parsedJob.setJobName(jobLine.getJobName());
        }
        
        if (jobLine.getJobQueue() != null)
        {
          parsedJob.setJobQueue(jobLine.getJobQueue());
        }
        
        if (jobLine.getSubmitTime() != null)
        {
          parsedJob.setSubmitTime(jobLine.getSubmitTime());
        }
        
        if (jobLine.getLaunchTime() != null)
        {
          parsedJob.setLaunchTime(jobLine.getLaunchTime());
        }
        
        if (jobLine.getTotalMaps() != null)
        {
          parsedJob.setTotalMaps(jobLine.getTotalMaps());
        }
        
        if (jobLine.getTotalReduces() != null)
        {
          parsedJob.setTotalReduces(jobLine.getTotalReduces());
        }
        
        if (jobLine.getFinishedMaps() != null)
        {
          parsedJob.setFinishedMaps(jobLine.getFinishedMaps());
        }
        
        if (jobLine.getFinishedReduces() != null)
        {
          parsedJob.setFinishedReduces(jobLine.getFinishedReduces());
        }
        
        if (jobLine.getFailedMaps() != null)
        {
          parsedJob.setFailedMaps(jobLine.getFailedMaps());
        }
        
        if (jobLine.getFailedReduces() != null)
        {
          parsedJob.setFailedReduces(jobLine.getFailedReduces());
        }
      }
    }
    
    public static void mergeTaskEntries(Job job, List<Task> taskEntries)
    {
      Map<String,Task> taskIdToTask = new HashMap<String,Task>();
            
      for (Task entry : taskEntries)
      {
        String taskId = entry.getTaskId().toString();
        
        if (taskId == null)
        {
          throw new RuntimeException("Missing task ID");
        }
        
        Task task = null;
        
        if (!taskIdToTask.containsKey(taskId))
        { 
          taskIdToTask.put(taskId,new Task());
        }
        
        task = taskIdToTask.get(taskId);
        
        if (task.getAttempts() == null)
        {
          task.setAttempts(new ArrayList<Attempt>());
        }
        
        if (entry.getTaskId() != null)
        {
          task.setTaskId(entry.getTaskId());
        }

        if (entry.getTaskStatus() != null)
        {
          task.setTaskStatus(entry.getTaskStatus());
        }
        
        if (entry.getStartTime() != null)
        {
          task.setStartTime(entry.getStartTime());
        }
        
        if (entry.getFinishTime() != null)
        {
          task.setFinishTime(entry.getFinishTime());
        }
        
        if (entry.getJobId() != null)
        {
          task.setJobId(entry.getJobId());
        }
        
        if (entry.getType() != null)
        {
          task.setType(entry.getType());
        }
      }
      
      List<Task> tasks = new ArrayList<Task>(taskIdToTask.values());
      
      Collections.sort(tasks, new Comparator<Task>() {
        @Override
        public int compare(Task o1, Task o2)
        {
          return o1.getTaskId().toString().compareTo(o2.getTaskId().toString());
        }
      });
      
      job.setTasks(tasks);
    }
    
    /**
     * Merges together attempt data having the same task attempt ID so all the data for an attempt is in a single record.
     * 
     * @param values
     * @param context
     * @return
     */
    public static void mergeTaskAttemptEntries(Job job, Iterable<Attempt> attemptEntries/*, final Context context*/)
    {
      // merge together the entries for each task attempt
      Map<String,Attempt> taskAttemptIdToAttempt = new HashMap<String,Attempt>();      
      for (Attempt attempt : attemptEntries)
      {        
        Attempt mergedAttempt;
        if (!taskAttemptIdToAttempt.containsKey(attempt.getTaskAttemptId().toString()))
        {
          mergedAttempt = new Attempt();
          mergedAttempt.setCounters(new HashMap<CharSequence,Long>());
          mergedAttempt.setDerived(new DerivedAttemptData());
          taskAttemptIdToAttempt.put(attempt.getTaskAttemptId().toString(),mergedAttempt);
        }
        else
        {
          mergedAttempt = taskAttemptIdToAttempt.get(attempt.getTaskAttemptId().toString());
        }              
        
        if (attempt.getType() != null)
        {
          mergedAttempt.setType(attempt.getType());
        }
        
        if (attempt.getJobId() != null)
        {
          mergedAttempt.setJobId(attempt.getJobId());
        }
        
        if (attempt.getTaskId() != null)
        {
          mergedAttempt.setTaskId(attempt.getTaskId());
        }
        
        if (attempt.getTaskAttemptId() != null)
        {
          mergedAttempt.setTaskAttemptId(attempt.getTaskAttemptId());
        }
        
        if (attempt.getStartTime() != null)
        {
          // take the later start time in case there are multiple
          if (mergedAttempt.getStartTime() == null || mergedAttempt.getStartTime() < attempt.getStartTime())
          {
            mergedAttempt.setStartTime(attempt.getStartTime());
          }
        }
        
        if (attempt.getFinishTime() != null)
        {
          // take the later finish time in case there are multiple
          if (mergedAttempt.getFinishTime() == null || mergedAttempt.getFinishTime() < attempt.getFinishTime())
          {
            mergedAttempt.setFinishTime(attempt.getFinishTime());
          }
        }
        
        if (attempt.getShuffleFinished() != null)
        {
          // take the later finish time in case there are multiple
          if (mergedAttempt.getShuffleFinished() == null || mergedAttempt.getShuffleFinished() < attempt.getShuffleFinished())
          {
            mergedAttempt.setShuffleFinished(attempt.getShuffleFinished());
          }
        }
        
        if (attempt.getSortFinished() != null)
        {
          // take the later finish time in case there are multiple
          if (mergedAttempt.getSortFinished() == null || mergedAttempt.getSortFinished() < attempt.getSortFinished())
          {
            mergedAttempt.setSortFinished(attempt.getSortFinished());
          }
        }
                
        if (attempt.getTaskStatus() != null)
        {
          mergedAttempt.setTaskStatus(attempt.getTaskStatus());
        }
        
        if (attempt.getCounters() != null && attempt.getCounters().size() > 0)
        {
          mergedAttempt.setCounters(attempt.getCounters());
        }
      }
      
      // filter out bad data
      Collection<Attempt> filteredAttempts = Collections2.filter(taskAttemptIdToAttempt.values(), new Predicate<Attempt>() {
        @Override
        public boolean apply(Attempt attempt)
        {
          if (attempt.getTaskAttemptId() == null)
          {
            System.out.println("Did not find task attempt ID");
            return false;
          }
          
          if (attempt.getTaskStatus() == null)
          {
            // The logs can sometimes be cut off, just count this and hopefully it isn't significant.  
            // The task probably didn't execute in this case.
            //context.getCounter("Job Parsing", "Missing status").increment(1);
            System.out.println("Did not find status for attempt " + attempt.getTaskAttemptId());
            return false;
          }
          
          if (attempt.getStartTime() == null)
          {
            // The logs can sometimes be cut off, just count this and hopefully it isn't significant.  
            // The task probably didn't execute in this case.
            //context.getCounter("Job Parsing", "Missing startTime").increment(1);
            System.out.println("Did not find startTime for attempt " + attempt.getTaskAttemptId());
            return false;
          }
          
          if (attempt.getFinishTime() == null)
          {
            // The logs can sometimes be cut off, just count this and hopefully it isn't significant.  
            // The task probably didn't execute in this case.
            //context.getCounter("Job Parsing", "Missing finishTime").increment(1);
            System.out.println("Did not find finishTime for attempt " + attempt.getTaskAttemptId());
            return false;
          }
          
          if (attempt.getFinishTime() < attempt.getStartTime())
          {
            //context.getCounter("Job Parsing", "Finish time before start time").increment(1);
            System.out.println("Finish time is before start for attempt " + attempt.getTaskAttemptId());
            return false;
          }
          
          return true;
        }
      });
      
      // map to look up task by task id
      Map<String,Task> taskIdToTask = new HashMap<String,Task>();
      for (Task task : job.getTasks())
      {
        taskIdToTask.put(task.getTaskId().toString(), task);
      }
      
      // collect each of the attempts and add to the corresponding task
      for (Attempt attempt : filteredAttempts)
      {
        Task task = taskIdToTask.get(attempt.getTaskId().toString());
        
        if (task == null)
        {
          throw new RuntimeException("Could not find task");
        }
        
        if (task.getAttempts() == null)
        {
          task.setAttempts(new ArrayList<Attempt>());
        }
        
        task.getAttempts().add(attempt);
      }
      
      for (Task task : job.getTasks())
      {
        if (task.getAttempts().size() > 0)
        {
          // sort attempts by start time
          Collections.sort(task.getAttempts(),new Comparator<Attempt>() {
            @Override
            public int compare(Attempt o1, Attempt o2)
            {
              return o1.getStartTime().compareTo(o2.getStartTime());
            }
          });
          
          boolean foundSuccess = false;
          
          // For simplicity we'll say that all attempts which are not successful are excess.
          // In reality there could be some overlapping successful attempts, but we'll ignore this
          // because it should be rare.
          
          for (Attempt attempt : task.getAttempts())
          { 
            if (attempt.getStartTime() == 0 || attempt.getFinishTime() == 0)
            {
              attempt.setStartTime(null);
              attempt.setFinishTime(null);
              //context.getCounter("Job Parsing", "startTime or finishTime zero").increment(1);
            }
            else
            {
              ((DerivedAttemptData)attempt.getDerived()).setMinutes((attempt.getFinishTime() - attempt.getStartTime())/1000.0/60.0);
            }
            
            if (attempt.getCounters().containsKey(CPU_MILLISECONDS))
            {
              attempt.getDerived().setCpuMinutes(attempt.getCounters().get(CPU_MILLISECONDS)/1000.0/60.0);
            }
            
            if (attempt.getTaskStatus().equals("SUCCESS"))
            {
              ((DerivedAttemptData)attempt.getDerived()).setExcess(false);
              foundSuccess = true;
            }
            else
            {
              ((DerivedAttemptData)attempt.getDerived()).setExcess(true);
            }
          }
          
          // If none were successful then mark the first attempt as the non-excess one.
          if (task.getAttempts().size() > 0 && !foundSuccess)
          {
            ((DerivedAttemptData)task.getAttempts().get(0).getDerived()).setExcess(false);
          }
          
          // sort by task attempt id
          Collections.sort(task.getAttempts(),new Comparator<Attempt>() {
            @Override
            public int compare(Attempt o1, Attempt o2)
            {
              return o1.getTaskAttemptId().toString().compareTo(o2.getTaskAttemptId().toString());
            }
          });
        }
      }
    }
  }
}
