package com.cloudwick.mapreduce.iplookup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by bijay on 12/16/14.
 */
public class GeoLocationLookupDriver extends Configured implements Tool {

  @Override
  public int run(String[] args)
          throws IOException, ClassNotFoundException, InterruptedException {
    if (args.length != 2) {
      System.err.println("Usage: GeoLocationLookup <in> <out>");
      System.exit(2);
    }


    Configuration conf = getConf();
    Job job = Job.getInstance(conf);

    job.setJobName("GeoLocation Lookup");
    job.setJarByClass(GeoLocationLookupDriver.class);
    job.setMapperClass(GeoLocationLookupMapper.class);
    job.setReducerClass(GeoLocationLookupReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    job.addFileToClassPath(new Path("/home/bijay/Desktop/HADOOP/data/GeoLite2-City.mmdb"));
    job.addFileToClassPath(new Path("/home/bijay/Desktop/HADOOP/data/geoip2-2.1.0-sources.jar"));

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    int ret = job.waitForCompletion(true) ? 0 : 1;
    return ret;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new GeoLocationLookupDriver(), args);
    System.exit(res);

  }
}
