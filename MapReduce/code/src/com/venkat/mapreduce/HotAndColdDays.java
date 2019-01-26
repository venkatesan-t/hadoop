package com.venkat.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HotAndColdDays {

	public static class HotAndColdDaysMapper extends Mapper<LongWritable, Text, IntWritable, Text>
	{
		private IntWritable date = new IntWritable();
		private float minTemp;
		private float maxTemp;
		private Text hotDay = new Text("Hot Day");
		private Text coldDay = new Text("Cold Day");
		private String record;
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			record = value.toString();
			if(record.length() == 216)
			{
				date.set(Integer.parseInt(record.substring(6, 14).trim()));
				maxTemp = Float.parseFloat(record.substring(38, 44).trim());
				minTemp = Float.parseFloat(record.substring(46, 52).trim());
				if(maxTemp > 40F)
				{
					context.write(date, hotDay);
				}
				else if(minTemp < 10)
				{
					context.write(date, coldDay);
				}
			}			
		}
	}
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Hot and Cold Days");
		job.setMapperClass(HotAndColdDaysMapper.class);
		job.setJarByClass(HotAndColdDays.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
