package com.cloudwick.mapreduce.logprocessing;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by bijay on 11/26/14.
 */
public class LogStatusReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
  private IntWritable count = new IntWritable();

  public void reduce(Text key, Iterable<IntWritable> values, Context context)
          throws IOException, InterruptedException {

    int sum = 0;
    for (IntWritable val : values) {
      sum += val.get();
    }
    count.set(sum);
    context.write(key, count);
  }
}
