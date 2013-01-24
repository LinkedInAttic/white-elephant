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

package com.linkedin.whiteelephant.mapreduce.lib.job;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class StagedOutputJobExecutor
{
  private final ExecutorService executor;
  private final List<Future<Boolean>> jobs = new ArrayList<Future<Boolean>>();
  
  public StagedOutputJobExecutor(int jobConcurrency)
  {
    executor = Executors.newFixedThreadPool(jobConcurrency);
  }
  
  public void submit(StagedOutputJob job)
  {
    System.out.println("Submitting " + job.getJobName());
    jobs.add(executor.submit(job));
  }
  
  public void waitForCompletion() throws InterruptedException, ExecutionException
  {
    while (!executor.isTerminated())
    {
      int numComplete = 0;
      int jobCount = jobs.size();
      for (Future<Boolean> job : jobs)
      {
        try
        {
          Boolean success = job.get(30,TimeUnit.SECONDS);
          if (success != null)
          {
            if (success)
            {
              numComplete++;
            }
            else
            {
              System.out.println("One or more jobs failed!  Shutting down remaining jobs...");
              executor.shutdownNow();
              throw new RuntimeException("Job failed!");
            }
          }
        }
        catch (TimeoutException e)
        { 
        }
      }
            
      if (numComplete == jobCount)
      {
        System.out.println("Current set of jobs have completed");
        break;
      }
    }
    
    jobs.clear();
  }
  
  public void waitForCompletionThenShutdown() throws InterruptedException, ExecutionException
  {
    executor.shutdown();
    
    try
    {
      while (!executor.isTerminated())
      {
        for (Future<Boolean> job : jobs)
        {
          try
          {
            Boolean success = job.get(30,TimeUnit.SECONDS);
            if (success != null && !success)
            {
              System.out.println("One or more jobs failed!  Shutting down remaining jobs...");
              executor.shutdownNow();
              throw new RuntimeException("Job failed!");
            }
          }
          catch (TimeoutException e)
          { 
          }
        }
      }
    }
    finally
    {
      executor.shutdownNow();
    }
    
    jobs.clear();
  }
  
  public void shutdownNow()
  {
    executor.shutdownNow();
  }
}
