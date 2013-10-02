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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.linkedin.whiteelephant.mapreduce.lib.input.CombineDocumentFileFormat;
import com.linkedin.whiteelephant.mapreduce.lib.job.StagedOutputJob;
import com.linkedin.whiteelephant.mapreduce.lib.job.StagedOutputJobExecutor;
import com.linkedin.whiteelephant.util.JobStatsProcessing;

public class ParseJobConfs
{
  private final Logger _log;
  private final Properties _props;
  private final FileSystem _fs;
  private final String _name;
  
  private final String _confsOutputPathRoot;
  private final String _logsRoot;
  private final String _clusterNames;
  private final int _numDays;
  private final int _numDaysForced;
  private final boolean _incremental;
  
  public ParseJobConfs(String name, Properties props) throws IOException
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

    _confsOutputPathRoot = (String)_props.get("confs.output.path");
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
            
      List<JobStatsProcessing.ProcessingTask> processingTasks = JobStatsProcessing.getTasks(_fs, _logsRoot, clusterName, _confsOutputPathRoot, "xml", _incremental, _numDays, _numDaysForced);
      
      for (JobStatsProcessing.ProcessingTask task : processingTasks)
      {      
        List<String> inputPaths = new ArrayList<String>();
        inputPaths.add(task.inputPathFormat);
        
        String outputPath = task.outputPath;
        
        final StagedOutputJob job = StagedOutputJob.createStagedJob(
           _props,
           _name + "-parse-confs-" + task.id,
           inputPaths,
           "/tmp" + outputPath,
           outputPath,
           _log);
        
        job.getConfiguration().set("jobs.output.path", _confsOutputPathRoot);
        job.getConfiguration().set("logs.cluster.name", clusterName);
                
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(NullWritable.class);
  
        job.setInputFormatClass(CombineDocumentFileFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);
  
        AvroJob.setOutputKeySchema(job, JobConf.SCHEMA$);
        
        job.setNumReduceTasks(0);
   
        job.setMapperClass(ParseJobConfs.TheMapper.class);
        
        executor.submit(job);
      }
      
      executor.waitForCompletion();
    }
  }
  
  public static class TheMapper extends Mapper<Text, BytesWritable, AvroWrapper<JobConf>, NullWritable> 
  {

    private Logger _log = Logger.getLogger(TheMapper.class);
    private static Pattern jobPattern = Pattern.compile("job_\\d+_\\d+");
      
    String _clusterName;

    private DocumentBuilder builder;

    @Override
    protected void setup(Context context)
    {
      _clusterName = context.getConfiguration().get("logs.cluster.name");
      try {
         builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      } catch (ParserConfigurationException e) {
        throw new RuntimeException(e);
      }
    }
    
    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException 
    {
      JobConf jobConf = new JobConf();
      String filename = key.toString();
      jobConf.setPath(filename);
        
      Matcher jobMatcher = jobPattern.matcher(filename);
       
      if (!jobMatcher.find()){
        throw new RuntimeException("Expected to find jobId in the filename. Aborting");
      }

      String jobId = jobMatcher.group();
      jobConf.setJobId(jobId);
      jobConf.setCluster(_clusterName);
        
      Map<CharSequence, CharSequence> conf = getConfigurationMap(value); 
        
      jobConf.setConfiguration(conf);
        
      context.write(new AvroKey<JobConf>(jobConf), NullWritable.get());
    }

    private Map<CharSequence, CharSequence> getConfigurationMap(BytesWritable bytes) {
      InputStream stream = new ByteArrayInputStream(bytes.getBytes(), 0, bytes.getLength());
      Document doc = null;
      try {
        doc = builder.parse(stream);
      } catch (SAXException e) {
        e.printStackTrace();
        return null;
      } catch (IOException e) {
        e.printStackTrace();
        return null;
      }
      NodeList children = doc.getElementsByTagName("configuration");
      Element child = (Element) children.item(0);
      NodeList properties = child.getElementsByTagName("property");
      Map<CharSequence, CharSequence> conf = new HashMap<CharSequence, CharSequence>(properties.getLength());
      for (int i = 0; i < properties.getLength(); i++)
      {
        Element property = (Element) properties.item(i);
        String name = property.getElementsByTagName("name").item(0).getTextContent();
        String value = property.getElementsByTagName("value").item(0).getTextContent();
        conf.put(name, value);
      }
      return conf;
    }
  }
}
