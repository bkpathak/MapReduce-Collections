package org.bkpathak.mapreduce.customloginputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by bijay on 12/5/14.
 */
public class LogInputFormatDriver extends Configured implements Tool {

  @Override
  public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    if (args.length != 2) {
      System.out.printf("Usage: " + this.getClass().getName());
      System.exit(-1);
    }

    Configuration conf = getConf();
    Job job = Job.getInstance();

    job.setJobName("Custom Log Input format");
    job.setJarByClass(LogInputFormat.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setInputFormatClass(LogInputFormat.class);
    job.setNumReduceTasks(0);
    boolean success = job.waitForCompletion(true);
    return (success ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    int exitCode = ToolRunner.run(conf, new LogInputFormatDriver(), args);
    System.exit(exitCode);
  }
}
