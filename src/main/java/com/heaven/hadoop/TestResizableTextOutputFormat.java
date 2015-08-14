package com.heaven.hadoop;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class TestResizableTextOutputFormat extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TestResizableTextOutputFormat(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance();
		job.getConfiguration().set("mapreduce.job.queuename", "qc");
		job.setNumReduceTasks(100);
		job.getConfiguration().setLong("mapreduce.job.split.metainfo.maxsize", Long.MAX_VALUE);
        		
		job.setMaxMapAttempts(180);
		
		job.setMapSpeculativeExecution(false);
		job.setReduceSpeculativeExecution(false);
		
		job.setJobName("Test Resizable TextOutputFormat");
		job.setJarByClass(TestResizableTextOutputFormat.class);
		

		
		job.setMapperClass(TestMapper.class);
		job.setReducerClass(TestReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		return job.waitForCompletion(true) ? 0:1;
	}


	static class TestMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
		
		@Override
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	static class TestReducer extends Reducer<LongWritable, Text, Text, NullWritable>{
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException {
				for(Text value: values){
					context.write(value, NullWritable.get());
				}
		}
	}
}
