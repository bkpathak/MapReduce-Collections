package com.cloudwick.mapreduce.secondarysort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by bijay on 12/18/14.
 */
public class SecondarySortDriver extends Configured implements Tool {

  @Override
  public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output> \n", getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    Configuration conf = getConf();
    Job job = Job.getInstance(conf);

    job.setJobName("Secondary Sort");
    job.setJarByClass(getClass());

    job.setMapperClass(SecondarySortMapper.class);
    job.setReducerClass(SecondarySortReducer.class);

    job.setMapOutputKeyClass(CompositeKey.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setOutputKeyClass(CompositeKey.class);
    job.setOutputValueClass(NullWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setPartitionerClass(CustomPartitioner.class);
    job.setGroupingComparatorClass(GroupComparator.class);
    job.setSortComparatorClass(KeyComparator.class);

    return job.waitForCompletion(true) ? 0 : 1;

  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new SecondarySortDriver(), args);
    System.exit(res);
  }
}
