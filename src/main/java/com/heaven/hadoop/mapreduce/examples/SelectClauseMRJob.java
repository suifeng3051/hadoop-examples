package com.heaven.hadoop.mapreduce.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mortbay.log.Log;

public class SelectClauseMRJob extends Configuration implements Tool {
	public static class SelectClauseMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		public void Map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 解析value并选择想要数据的项目
			String[] selectFieldArray = AirlineDataUtils.getSelectResultPerRow(value);
			// 拼接各个项目成一行
			String formateArray = AirlineDataUtils.mergeStringArray(selectFieldArray, ",");
			Log.info(formateArray);
			context.write(NullWritable.get(), new Text(formateArray));
		}
	}

	@Override
	public void setConf(Configuration conf) {

	}

	@Override
	public Configuration getConf() {
		Configuration conf = new Configuration();
		return conf;
	}

	@Override
	public int run(String[] allArgs2) throws Exception {
		String[] allArgs = { "hdfs://192.168.29.131:9000/user/data/airlinedata.bz2", "hdfs://192.168.29.131:9000/user/output/airlinedata5" };
		Job job = Job.getInstance(getConf());
		job.setJarByClass(SelectClauseMRJob.class);
		job.setInputFormatClass(TextInputFormat.class);// 一行一行地读取数据
		job.setOutputFormatClass(TextOutputFormat.class);// 输出的为text文件
		job.setOutputKeyClass(NullWritable.class);// 如果不用NullWritable，默认key为tab制表符，而加上之后，输出的仅为用逗号分隔的值
		job.setOutputValueClass(Text.class);
		job.setMapperClass(SelectClauseMapper.class);
		job.setNumReduceTasks(0);//这句是重点，只启动map任务，不需要reduce过程，节省了shuffle时间
		String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean status = job.waitForCompletion(true);
		if (status) {
			return 0;
		} else {
			return 1;
		}
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new SelectClauseMRJob(), args);
	}

}
