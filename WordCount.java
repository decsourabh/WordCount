/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.Hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Path out = new Path(args[1]);
    
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(out, "out1"));
    if (!job.waitForCompletion(true)) {
      System.exit(1);
    }
    
    Job job1=new Job(conf,"sort by frequency");
    job1.setJarByClass(WordCount.class);
    job1.setMapperClass(MyMapper2.class);
    job1.setNumReduceTasks(1);
    job1.setSortComparatorClass(LongWritable.DecreasingComparator.class);
    job1.setOutputKeyClass(LongWritable.class);
    job1.setOutputValueClass(Text.class);
    job1.setInputFormatClass(SequenceFileInputFormat.class);
    FileInputFormat.addInputPath(job1, new Path(out, "out1"));
    FileOutputFormat.setOutputPath(job1, new Path(out, "out2"));
    if (!job1.waitForCompletion(true)) {
      System.exit(1);
    }

  }
}
