package org.bkpathak.mapreduce.custominputformat;

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
 * Created by bijay on 12/4/14.
 */
public class FixedWidthColumnDriver extends Configured implements Tool {

  @Override
  public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    if (args.length != 2) {
      System.out.printf("Usage: " + this.getClass().getName() +
              "<in dir> <out dir> \n");
      System.exit(-1);
    }

    Configuration conf = getConf();
    /**
     * Provide the information for the each record, i.e,
     * The number of field in the record including
     */
    FixedWidthColumnInputFormat.setRecordLength(conf, 50);
    FixedWidthColumnRecordReader.setNumValue(conf, 4);
    FixedWidthColumnRecordReader.setValuesWidth(conf, "7", "25", "10", "8");


    Job job = Job.getInstance(conf);

    job.setJobName("Fixed column width format");
    job.setJarByClass(FixedWidthColumnDriver.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setInputFormatClass(FixedWidthColumnInputFormat.class);
    job.setNumReduceTasks(0);


    int ret = job.waitForCompletion(true) ? 0 : 1;
    return ret;

  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new FixedWidthColumnDriver(), args);
    System.exit(res);
  }
}
