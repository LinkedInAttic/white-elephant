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

package com.linkedin.whiteelephant.mapreduce.lib.input;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class CombinedTextInputFormat extends CombineFileInputFormat<LongWritable, Text> {
  
  public static class MyLineRecordReader extends LineRecordReader
  {
    private CombineFileSplit inputSplit;
    private Integer idx;
    
    public MyLineRecordReader(CombineFileSplit inputSplit, TaskAttemptContext context, Integer idx)
    {
      this.inputSplit = inputSplit;
      this.idx = idx;
    }
    
    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws java.io.IOException
    {
      this.inputSplit = (CombineFileSplit)genericSplit;
      
      FileSplit fileSplit = new FileSplit(this.inputSplit.getPath(idx), 
                                          this.inputSplit.getOffset(idx), 
                                          this.inputSplit.getLength(idx), 
                                          this.inputSplit.getLocations());
      
      super.initialize(fileSplit, context);
    }
  }
  
  
  @Override
  public RecordReader<LongWritable, Text> createRecordReader(org.apache.hadoop.mapreduce.InputSplit inputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext taskAttemptContext) throws IOException {
    return new CombineFileRecordReader<LongWritable, Text>((CombineFileSplit) inputSplit, taskAttemptContext, MyLineRecordReader.class);
  }
}