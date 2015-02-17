package org.bkpathak.mapreduce.logcustomwritable;

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
 * Created by bijay on 11/29/14.
 */
public class LogCustomWritableDriver extends Configured implements Tool {

  @Override
  public int run(String[] args)
          throws IOException, ClassNotFoundException, InterruptedException {

    if (args.length != 2) {
      System.out.printf("Usage" + this.getClass().getName() + "<in> <out> \n");
      System.exit(2);
    }

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);

    job.setJobName("Log custom writable");
    job.setJarByClass(LogCustomWritableDriver.class);
    job.setMapperClass(LogCustomMapper.class);
    job.setReducerClass(LogCustomReducer.class);

    job.setMapOutputKeyClass(LogWritable.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setOutputKeyClass(LogWritable.class);
    job.setOutputValueClass(IntWritable.class);


    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    int ret = job.waitForCompletion(true) ? 0 : 1;
    return ret;

  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new LogCustomWritableDriver(), args);
    System.exit(res);

  }
}
