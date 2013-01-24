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

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.hadoop.fs.PathFilter;

/**
 *
 */
public class StagedOutputJob extends Job implements Callable<Boolean>
{
    private final String stagingPrefix;
    private final Logger log;
    
    private static String HADOOP_PREFIX = "hadoop-conf.";

    /**
     * Creates a job which using a temporary staging location for the output data.
     * The data is only copied to the final output directory on successful completion
     * of the job.  This prevents existing output data from being overwritten unless
     * the job completes successfully.
     * 
     * @param props Job properties
     * @param jobName Name of the job
     * @param inputPaths Input paths job will be reading from
     * @param stagingLocation Where output of job should be staged
     * @param outputPath The final output location for the data
     * @param log The logger
     * @return The job
     */
    public static StagedOutputJob createStagedJob(
            Properties props,
            String jobName,
            List<String> inputPaths,
            String stagingLocation,
            String outputPath,
            final Logger log
    )
    {
        Configuration config = createConfigurationFromProps(props);

        final StagedOutputJob retVal;
        try {
            retVal = new StagedOutputJob(config, stagingLocation, log);

            retVal.setJobName(jobName);
            retVal.setJarByClass(getCallersClass());
            FileInputFormat.setInputPathFilter(retVal, HiddenFilePathFilter.class);

        }
        catch (IOException e) {
            log.error("IOException when making a job, wtf?", e);
            throw new RuntimeException(e);
        }

        try {
            FileInputFormat.setInputPaths(
                    retVal,
                    StringUtils.join(inputPaths.iterator(),",")
            );
        }
        catch (IOException e) {
            log.error("Unable to set up input paths.", e);
            throw new RuntimeException(e);
        }
                
        FileOutputFormat.setOutputPath(retVal, new Path(outputPath));

        return retVal;
    }
    
    public static Configuration createConfigurationFromProps(Properties props)
    {
      final Configuration config = new Configuration();
      for (String key : props.stringPropertyNames()) {            
                      
          String newKey = key;
          String value = props.getProperty(key);
          
          if (key.toLowerCase().startsWith(HADOOP_PREFIX)) {
            newKey = key.substring(HADOOP_PREFIX.length());
            config.set(newKey, value);
          }
      }
      return config;
    }
    
    public StagedOutputJob(String stagingPrefix, Logger log) throws IOException
    {
        super();
        this.stagingPrefix = stagingPrefix;
        this.log = log;
    }

    public StagedOutputJob(Configuration conf, String stagingPrefix, Logger log) throws IOException
    {
        super(conf);
        this.stagingPrefix = stagingPrefix;
        this.log = log;
    }

    public StagedOutputJob(Configuration conf, String jobName, String stagingPrefix, final Logger log) throws IOException
    {
        super(conf, jobName);
        this.stagingPrefix = stagingPrefix;
        this.log = log;
    }

    @Override
    public Boolean call() throws Exception
    {
      try
      {
        boolean success = false;
      
        success = waitForCompletion(false);
        
        String jobId = "?";
        if (getJobID() != null)
        {
          jobId = String.format("job_%s_%d",getJobID().getJtIdentifier(), getJobID().getId());
        }
        
        if (success)
        {
          
          System.out.println(String.format("Job %s with ID %s succeeded! Tracking URL: %s", getJobName(), jobId, this.getTrackingURL()));
        }
        else
        {
          System.out.println(String.format("Job %s with ID %s failed! Tracking URL: %s", getJobName(), jobId, this.getTrackingURL()));
        }
        
        return success;
      }
      catch (Exception e)
      {
        System.out.println("Exception: " + e.toString());
        throw new Exception(e);
      }
    }
    
    @Override
    public boolean waitForCompletion(boolean verbose) throws IOException, InterruptedException, ClassNotFoundException
    {
        final Path actualOutputPath = FileOutputFormat.getOutputPath(this);
        final Path stagedPath = new Path(String.format("%s/%s/staged", stagingPrefix, System.currentTimeMillis()));

        FileOutputFormat.setOutputPath(
                this,
                stagedPath
        );

        final Thread hook = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    killJob();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        Runtime.getRuntime().addShutdownHook(hook);

        final boolean retVal = super.waitForCompletion(verbose);
        Runtime.getRuntime().removeShutdownHook(hook);

        if (retVal) {
            FileSystem fs = actualOutputPath.getFileSystem(getConfiguration());

            fs.mkdirs(actualOutputPath);

            log.info(String.format("Deleting data at old path[%s]", actualOutputPath));
            fs.delete(actualOutputPath, true);

            log.info(String.format("Moving from staged path[%s] to final resting place[%s]", stagedPath, actualOutputPath));
            return fs.rename(stagedPath, actualOutputPath);
        }

        log.warn("retVal was false for some reason...");
        return retVal;
    }
    
    private static Class<?> getCallersClass()
    {
        StackTraceElement[] stack = Thread.currentThread().getStackTrace();

        boolean foundSelf = false;
        for (StackTraceElement element : stack) {
            if (
                    foundSelf &&
                    ! StagedOutputJob.class.getName().equals(element.getClassName())
                    ) {
                try {
                    return Class.forName(element.getClassName());
                }
                catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
            else if (
                StagedOutputJob.class.getName().equals(element.getClassName()) &&
                    "getCallersClass".equals(element.getMethodName())
                    ) {
                foundSelf = true;
            }
        }

        return StagedOutputJob.class;
    }
    
    public static class HiddenFilePathFilter implements PathFilter
    {
        @Override
        public boolean accept(Path path)
        {
            String name = path.getName();
            return ! name.startsWith("_") &&
                   ! name.startsWith(".");
        }
    }
}
