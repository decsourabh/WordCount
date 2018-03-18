package com.Hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper2 extends Mapper<Text, LongWritable, LongWritable, Text>{
	public void map(Text key, LongWritable value, Context ctx) throws IOException, InterruptedException {
     ctx.write(value, key);
     
	}
	
}