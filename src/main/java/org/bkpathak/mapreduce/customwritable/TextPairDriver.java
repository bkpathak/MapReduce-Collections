package org.bkpathak.mapreduce.customwritable;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by bijay on 11/28/14.
 */
public class TextPairDriver extends Configured implements Tool {

  @Override
  public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    if (args.length != 2) {
      System.out.printf("Usage: " + this.getClass().getName() + "<input dir> <output dir>\n");
    }

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);

    job.setJobName("Text pair counter");
    job.setJarByClass(TextPairDriver.class);
    job.setMapperClass(TextPairMapper.class);
    job.setReducerClass(TextPairReducer.class);

    job.setMapOutputKeyClass(TextPairWritable.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setOutputKeyClass(TextPairWritable.class);
    job.setOutputKeyClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));


    int ret = job.waitForCompletion(true) ? 0 : 1;
    return ret;

  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new TextPairDriver(), args);
    System.exit(exitCode);
  }
}
