package com.cloudwick.mapreduce.custompartitioner;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by bijay on 12/16/14.
 */
public class StatusCodePartitioner<Text, IntWritable> extends Partitioner<Text, IntWritable> {

  @Override
  public int getPartition(Text key, IntWritable Value, int numReduceTasks) {
    return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
  }
}
