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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class CombineDocumentFileFormat extends CombineFileInputFormat<Text, BytesWritable>{
    
    public static class WholeFileRecordReader extends RecordReader<Text, BytesWritable>{
        private CombineFileSplit inputSplit;
        private Integer idx;
        private Text path;
        private BytesWritable document;
        private Configuration conf;
        private boolean read;
        
        public WholeFileRecordReader(CombineFileSplit inputSplit, TaskAttemptContext context, Integer idx)
        {
          this.inputSplit = inputSplit;
          this.idx = idx;
          this.conf = context.getConfiguration();
          this.read = false;
        }
        
        @Override
        public void close() throws IOException {
            // Don't need to do anything
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return path;
        }

        @Override
        public BytesWritable getCurrentValue() throws IOException, InterruptedException {
            return document;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            if (!read)
                return 0;
            else 
                return 1;
        }

        @Override
        public void initialize(InputSplit arg0, TaskAttemptContext arg1)
                throws IOException, InterruptedException {
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!read){
                Path file = inputSplit.getPath(idx);
                FileSystem fs = file.getFileSystem(conf);
                FSDataInputStream input = fs.open(file);
                byte[] bytes = new byte[(int) inputSplit.getLength(idx)];
                int offset = (int) inputSplit.getOffset(idx);
                int length = (int) inputSplit.getLength(idx);
                IOUtils.readFully(input, bytes, offset, length);
                
                document = new BytesWritable();
                document.set(bytes, offset, length);
                
                path = new Text(file.toString());
                read = true;
                return true;
            } else {
                return false;
            }
        }
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit arg0,
            TaskAttemptContext arg1) throws IOException {
        return new CombineFileRecordReader<Text, BytesWritable>((CombineFileSplit) arg0, arg1, WholeFileRecordReader.class);
    }

}
