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

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.linkedin.whiteelephant.parsing.Attempt;
import com.linkedin.whiteelephant.parsing.DerivedAttemptData;
import com.linkedin.whiteelephant.parsing.Job;
import com.linkedin.whiteelephant.parsing.JobStatus;
import com.linkedin.whiteelephant.parsing.Task;
import com.linkedin.whiteelephant.parsing.TaskStatus;
import com.linkedin.whiteelephant.parsing.TaskType;

public class LineParsing
{
  public enum AttemptParameter 
  {
    TASKID,
    TASK_ATTEMPT_ID,
    TASK_STATUS,
    START_TIME,
    FINISH_TIME,
    SHUFFLE_FINISHED,
    SORT_FINISHED
  }
  
  private static String quotedTextPattern = "\"([^\"]+)\"";
  private static Pattern jobLinePattern = Pattern.compile(String.format("^Job JOBID=%s.*",quotedTextPattern));    
  private static Pattern jobPattern = Pattern.compile("job_\\d+_\\d+");    
  private static Pattern parameterPattern = Pattern.compile("([A-Z_]+)=" + quotedTextPattern); 
  private static Pattern counterPattern = Pattern.compile("\\[\\(([A-Z_]+)\\)\\(.+?\\)\\((\\d+)\\)\\]");  
  private static Pattern taskPattern = Pattern.compile("task_(\\d+_\\d+)_[mr]_\\d+");    
  private static Pattern taskLinePattern = Pattern.compile(String.format("Task TASKID=%s TASK_TYPE=\"(MAP|REDUCE)\".+",quotedTextPattern));
  private static Pattern attemptLinePattern = Pattern.compile("^(Map|Reduce)Attempt TASK_TYPE=\"(MAP|REDUCE)\".+");
  
  public static Job tryParseJob(String line)
  {
    // these mess with our pattern matching
    line = line.replace("\\\"", "");
    
    Job job = null;
    
    Matcher m = jobLinePattern.matcher(line);
    if (m.matches())
    {
      job = new Job();
      
      job.setJobId(m.group(1));
      
      Matcher paramMatcher = parameterPattern.matcher(line);
      while (paramMatcher.find())
      {
        String name = paramMatcher.group(1);
        String value = paramMatcher.group(2);                
        maybeSetJobParam(job, name, value);
      }
    }
    else if (line.indexOf("USER=") >= 0)
    {
      Matcher jobMatcher = jobPattern.matcher(line);
      if (jobMatcher.find())
      {
        String jobId = jobMatcher.group();
        
        job = new Job();
        
        job.setJobId(jobId);
        
        Matcher paramMatcher = parameterPattern.matcher(line);
        while (paramMatcher.find())
        {
          String name = paramMatcher.group(1);
          String value = paramMatcher.group(2);                
          maybeSetJobParam(job, name, value);
        }
      }
    }
    
    return job;
  }
  
  private static void maybeSetJobParam(Job job, String name, String value)
  {
    if (name.equals("USER"))
    {
      job.setUser(value);
    }
    else if (name.equals("JOBNAME"))
    {
      job.setJobName(value);
    }
    else if (name.equals("JOB_QUEUE"))
    {
      job.setJobQueue(value);
    }
    else if (name.equals("JOB_STATUS"))
    {
      if (value.equals("SUCCESS") || value.equals("FAILURE"))
      {
        job.setJobStatus(JobStatus.valueOf(value));
      }
    }
    else if (name.equals("SUBMIT_TIME"))
    {
      job.setSubmitTime(Long.parseLong(value));
    }
    else if (name.equals("LAUNCH_TIME"))
    {
      job.setLaunchTime(Long.parseLong(value));
    }
    else if (name.equals("FINISH_TIME"))
    {
      job.setFinishTime(Long.parseLong(value));
    }
    else if (name.equals("TOTAL_MAPS"))
    {
      job.setTotalMaps(Integer.parseInt(value));
    }
    else if (name.equals("TOTAL_REDUCES"))
    {
      job.setTotalReduces(Integer.parseInt(value));
    }
    else if (name.equals("FINISHED_MAPS"))
    {
      job.setFinishedMaps(Integer.parseInt(value));
    }
    else if (name.equals("FINISHED_REDUCES"))
    {
      job.setFinishedReduces(Integer.parseInt(value));
    }
    else if (name.equals("FAILED_MAPS"))
    {
      job.setFailedMaps(Integer.parseInt(value));
    }
    else if (name.equals("FAILED_REDUCES"))
    {
      job.setFailedReduces(Integer.parseInt(value));
    }
  }
  
  public static Attempt tryParseAttempt(String line)
  {
    // these mess with our pattern matching
    line = line.replace("\\\"", "");
    
    Attempt attempt = null;
    
    Matcher m = attemptLinePattern.matcher(line);
    
    if (m.matches())
    {
      attempt = new Attempt();
            
      attempt.setCounters(new HashMap<CharSequence,Long>());
      
      attempt.setDerived(new DerivedAttemptData());
      
      attempt.setType(TaskType.valueOf(m.group(1).toUpperCase()));
      
      Matcher matcher = parameterPattern.matcher(line);
      
      while (matcher.find())
      {
        String name = matcher.group(1);
        String value = matcher.group(2);
        maybeSetAttemptParam(attempt,name,value);
      }
      
      matcher = counterPattern.matcher(line);
      
      while (matcher.find())
      {
        String name = matcher.group(1);
        String value = matcher.group(2);
        setAttemptCounter(attempt,name,Long.parseLong(value));        
      }
      
      if (attempt.getTaskId() != null)
      {
        Matcher taskMatcher = taskPattern.matcher(attempt.getTaskId());
        
        if (taskMatcher.matches())
        {
          String jobId = String.format("job_%s",taskMatcher.group(1));
          attempt.setJobId(jobId);
        }
        else
        {
          System.out.println("Could not match task ID for " + attempt.getTaskId());
          System.out.println("line: " + line);
        }
      }
      else
      {
        System.out.println("Could not find task ID");
        System.out.println("line: " + line);
        attempt = null;
      }
    }
    
    return attempt;
  }
  
  private static void maybeSetAttemptParam(Attempt attempt, String name, String value)
  {    
    try
    {
      AttemptParameter param = AttemptParameter.valueOf(name);
      
      if (param.equals(AttemptParameter.TASKID))
      {
        attempt.setTaskId(value);
      }
      else if (param.equals(AttemptParameter.TASK_ATTEMPT_ID))
      {
        attempt.setTaskAttemptId(value);
      }
      else if (param.equals(AttemptParameter.TASK_STATUS))
      {    
        attempt.setTaskStatus(TaskStatus.valueOf(value));
      }
      else if (param.equals(AttemptParameter.START_TIME))
      {
        attempt.setStartTime(Long.parseLong(value));
      }
      else if (param.equals(AttemptParameter.FINISH_TIME))
      {
        attempt.setFinishTime(Long.parseLong(value));
      }
      else if (param.equals(AttemptParameter.SHUFFLE_FINISHED))
      {
        attempt.setShuffleFinished(Long.parseLong(value));
      }
      else if (param.equals(AttemptParameter.SORT_FINISHED))
      {
        attempt.setSortFinished(Long.parseLong(value));
      }
    }
    catch (IllegalArgumentException e)
    {
      // ignore these, it means the enum isn't one we care about
    }
  }
  
  private static void setAttemptCounter(Attempt attempt, String name, Long value)
  {
    attempt.getCounters().put(name, value);
  }
   
  public static Task tryParseTask(String line)
  {
    // these mess with our pattern matching
    line = line.replace("\\\"", "");
    
    Task task = null;
    
    Matcher m = taskLinePattern.matcher(line);
    
    if (m.matches())
    {
      task = new Task();
      
      task.setType(TaskType.valueOf(m.group(2).toUpperCase()));
      
      Matcher matcher = parameterPattern.matcher(line);
      
      while (matcher.find())
      {
        String name = matcher.group(1);
        String value = matcher.group(2);
        maybeSetTaskParam(task,name,value);
      }
      
      if (task.getTaskId() != null)
      {
        Matcher taskMatcher = taskPattern.matcher(task.getTaskId());
        
        if (taskMatcher.matches())
        {
          String jobId = String.format("job_%s",taskMatcher.group(1));
          task.setJobId(jobId);
        }
        else
        {
          System.out.println("Could not match task ID for " + task.getTaskId());
          System.out.println("line: " + line);
        }
      }
      else
      {
        System.out.println("Could not find task ID");
        System.out.println("line: " + line);
        task = null;
      }
    }
    
    return task;
  }
  
  private static void maybeSetTaskParam(Task attempt, String name, String value)
  {    
    if (name.equals("TASKID"))
    {
      attempt.setTaskId(value);
    }
    else if (name.equals("TASK_STATUS"))
    {
      if (value.equals("SUCCESS") || value.equals("FAILURE"))
      {
        attempt.setTaskStatus(TaskStatus.valueOf(value));
      }
    }
    else if (name.equals("START_TIME"))
    {
      attempt.setStartTime(Long.parseLong(value));
    }
    else if (name.equals("FINISH_TIME"))
    {
      attempt.setFinishTime(Long.parseLong(value));
    }
  }
}
