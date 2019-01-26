package com.venkat.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MaxTemperature {

	public static class MaxTempMapper extends Mapper<LongWritable, Text, ShortWritable, ByteWritable> {
		private ShortWritable year = new ShortWritable();
		private ByteWritable temp = new ByteWritable();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] tokens = value.toString().split(" ");
			if(tokens.length == 2)
			{
				year.set(Short.parseShort(tokens[0]));
				temp.set(Byte.parseByte(tokens[1]));
				context.write(year, temp);
			}
		}
	}
	
	public static class MaxTempReducer extends Reducer<ShortWritable, ByteWritable, ShortWritable, ByteWritable> {
		private ByteWritable result = new ByteWritable();
		
		public void reduce(ShortWritable key, Iterable<ByteWritable> values, Context context) throws IOException, InterruptedException
		{
			byte maxTemp = -1;
			byte temp;
			for(ByteWritable value: values)
			{
				temp = value.get();
				if(temp > maxTemp)
				{
					maxTemp = temp;
				}
			}
			result.set(maxTemp);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Max Temperature");
		
		job.setMapperClass(MaxTempMapper.class);
		job.setCombinerClass(MaxTempReducer.class);
		job.setReducerClass(MaxTempReducer.class);
		job.setJarByClass(MaxTemperature.class);
		
		job.setMapOutputKeyClass(ShortWritable.class);
		job.setMapOutputValueClass(ByteWritable.class);
		job.setOutputKeyClass(ShortWritable.class);
		job.setOutputValueClass(ByteWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
