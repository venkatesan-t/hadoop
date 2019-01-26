package com.venkat.mapreduce;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AirportsList {
	
	public static class AirportsMapper extends Mapper<LongWritable, Text, VIntWritable, Text>
	{
		VIntWritable apId = new VIntWritable();
		Text apNm = new Text();
		String formattedAirportName;
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			//write dp logic
			String[] tokens = value.toString().split(",");
			if(tokens.length == 12 && tokens[3].trim().equalsIgnoreCase("INDIA"))
			{
				formattedAirportName = tokens[1].trim() + "," +
						tokens[2].trim() + "," + tokens[3].trim();
				apId.set(Integer.parseInt(tokens[0]));
				apNm.set(formattedAirportName);
				context.write(apId, apNm);
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
		conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, GzipCodec.class, CompressionCodec.class);
		
		Job job = Job.getInstance(conf, "Airports Count");
		
		job.setMapperClass(AirportsMapper.class);
		job.setJarByClass(AirportsList.class);
		
		job.setMapOutputKeyClass(VIntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
