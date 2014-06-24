package de.uni_stuttgart.ipvs_as.test.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Runner extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Expected 3 params: prefix, input and output");
			System.exit(-1);
		}

		Configuration conf = getConf();

		Job job = Job.getInstance(conf);
		job.setJarByClass(Runner.class);
		job.setJobName("MapReduceWSI_EndToEndTest");

		// MapReduceWSI always passes the HDFS prefix for this scope as arg0
		final String baseHDFSPath = args[0];
		final String inputHDFSPath = String.format("%s/%s", baseHDFSPath,
				args[1]);
		final String outputHDFSPath = String.format("%s/%s", baseHDFSPath,
				args[2]);

		FileInputFormat.setInputPaths(job, new Path(inputHDFSPath));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(outputHDFSPath));
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(TestMapper.class);
		job.setReducerClass(TestReducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
		return job.isSuccessful() ? 1 : 0
	}

	public static void main(String[] args) throws Exception {
		Runner driver = new Runner();
		final int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}