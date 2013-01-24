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

package com.linkedin.whiteelephant;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.linkedin.whiteelephant.analysis.ComputeUsagePerHour;
import com.linkedin.whiteelephant.mapreduce.lib.job.StagedOutputJobExecutor;
import com.linkedin.whiteelephant.parsing.ParseJobsFromLogs;

public class ProcessLogs implements Runnable
{
  private final Logger _log;
  private final Properties _props;
  private double _progress;  
  
  private final int _jobConcurrency;
  private final StagedOutputJobExecutor _executor;
  
  private final ParseJobsFromLogs parseJobs;
  private final ComputeUsagePerHour usagePerHour;
  
  public ProcessLogs(String name, Properties props) throws IOException {
    _log = Logger.getLogger(name);
    _props = props;    
    
    if (_props.get("job.concurrency") == null) {
      throw new IllegalArgumentException("job.concurrency is not specified.");
    }
    
    // set log level for these classes to error to suppress spewing warnings about splits 
    org.apache.log4j.Logger.getLogger("org.apache.hadoop.mapreduce.split.JobSplitWriter").setLevel(Level.ERROR);
    org.apache.log4j.Logger.getLogger("org.apache.hadoop.mapreduce.split.SplitMetaInfoReader").setLevel(Level.ERROR);

    _jobConcurrency = Integer.parseInt((String)_props.get("job.concurrency"));
    _executor = new StagedOutputJobExecutor(_jobConcurrency);
    
    parseJobs = new ParseJobsFromLogs(name, props);
    usagePerHour = new ComputeUsagePerHour(name, props);
  }
  
  public void run()
  {
    _log.info(String.format("Starting %s", getClass().getSimpleName()));

    try
    {
      System.out.println("Parsing logs");
      
      parseJobs.execute(_executor);
      usagePerHour.execute(_executor);
      
      _executor.waitForCompletionThenShutdown();
      
      System.out.println("All tasks have completed!");
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
    }
    catch (ExecutionException e)
    {
      e.printStackTrace();
    }
  } 
        
  public double getProgress()
  {
    return _progress;
  }
  
  public void cancel()
  {    
    _executor.shutdownNow();
  }
  
  private static void loadProperties(Properties props, String fileName) throws IOException
  {
    FileInputStream propStream = new FileInputStream(fileName);
    props.load(propStream);
    propStream.close();
  }
  
  public static void main(String[] args) throws IOException
  {    
    if (args.length == 0)
    {
      System.out.println("The job file name is required");
      System.exit(1);
    }
    else if (args.length > 1)
    {
      System.out.println("Too many arguments.  Only the job file name is required");
      System.exit(1);
    }
    
    String jobName = args[0];
    File jobFile = new File(jobName);
    
    File[] propFiles = new File(".").listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name)
      {
        return name.endsWith(".properties");
      }
    });
    
    Properties props = new Properties();
    
    for (File propFile : propFiles)
    {
      System.out.println("Loading configuration from " + propFile.getAbsolutePath());
      loadProperties(props,propFile.getAbsolutePath());
    }
    
    if (jobFile.exists())
    {
      System.out.println("Loading configuration from " + jobFile.getAbsolutePath());
      loadProperties(props,jobFile.getAbsolutePath());
    }
    else
    {
      System.out.println("File " + jobName + " not found");
      System.exit(1);
    }    
    
    new ProcessLogs(jobName,props).run();
  }
}
