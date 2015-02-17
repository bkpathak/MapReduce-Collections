package org.bkpathak.mapreduce.secondarysort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by bijay on 12/18/14.
 */
public class CustomPartitioner extends Partitioner<CompositeKey, NullWritable> {

  @Override
  public int getPartition(CompositeKey key, NullWritable value, int numReduceTasks) {
    return key.getIpAddress().hashCode() % numReduceTasks;
  }
}
