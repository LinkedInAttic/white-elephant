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

package com.linkedin.whiteelephant.util;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class JobStatsProcessing
{
  // The logs are uploaded to directories named according to approximately the GMT time of the job submission.
  private static TimeZone timeZone = TimeZone.getTimeZone("GMT");
  
  public static List<ProcessingTask> getTasks(FileSystem fs, String logsRoot, String clusterName, String outputPathRoot, boolean incremental, int numDays, int numDaysForced) throws IOException
  {    
    Calendar cal = Calendar.getInstance(timeZone);
    
    SimpleDateFormat yearFormat = new SimpleDateFormat("yyyy");
    SimpleDateFormat dayFormat = new SimpleDateFormat("MMdd");
    SimpleDateFormat idFormat = new SimpleDateFormat("yyyy-MM-dd");     
    
    yearFormat.setTimeZone(timeZone);
    dayFormat.setTimeZone(timeZone);
    idFormat.setTimeZone(timeZone);
    
    List<ProcessingTask> processingTasks = new ArrayList<ProcessingTask>();
    
    numDays = Math.max(numDays, numDaysForced);
    
    // Start processing previous day of data since current day isn't yet finished.  Unless we are aggregating hourly data there is no point.
    cal.add(Calendar.DAY_OF_MONTH, -1);
    
    int numPaths = 0;
    long totalLength = 0;
    for (int i=0; i<numDays; i++, cal.add(Calendar.DAY_OF_MONTH, -1))
    { 
      Date date = cal.getTime();
      
      String pathFormat = String.format("%s/%s/daily/*/%s/%s/*.log",logsRoot,clusterName,yearFormat.format(date),dayFormat.format(date));
      FileStatus[] stats = fs.globStatus(new Path(pathFormat));
      
      StringBuilder msg = new StringBuilder(pathFormat + " => " + stats.length + " files");
            
      String outputPathForDay = String.format("%s/%s/%s/%s",outputPathRoot,clusterName,yearFormat.format(date),dayFormat.format(date));
             
      if (stats.length > 0)
      {
        if (!incremental || !fs.exists(new Path(outputPathForDay)) || i<numDaysForced)
        {
          for (FileStatus stat : stats)
          {         
            totalLength += stat.getLen();
            numPaths++;
          }
          
          String id = clusterName + "-" + idFormat.format(date);
          
          System.out.println(msg);
          
          processingTasks.add(new ProcessingTask(id,pathFormat,outputPathForDay, totalLength));
        }
        else if (incremental && fs.exists(new Path(outputPathForDay)))
        {
          msg.append(" (skipping)");
          System.out.println(msg);
        }
      }      
    }
    
    System.out.println("Found " + numPaths + " paths to process, totalling " + totalLength + " bytes (" + (totalLength/(1024*1024*1024)) + " gigabytes)");
    
    return processingTasks;
  }
  
  
  public static class ProcessingTask
  {
    public final String id;
    public final String inputPathFormat;
    public final String outputPath;
    public final long totalLength;
    
    public ProcessingTask(String id, String inputPathFormat, String outputPath, long totalLength)
    {
      this.id = id;
      this.inputPathFormat = inputPathFormat;
      this.outputPath = outputPath;
      this.totalLength = totalLength;
    }
  }
}
