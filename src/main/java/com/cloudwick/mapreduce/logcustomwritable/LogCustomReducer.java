package com.cloudwick.mapreduce.logcustomwritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.InputMismatchException;

/**
 * Created by bijay on 11/29/14.
 */
public class LogCustomReducer
        extends Reducer<LogWritable, IntWritable, LogWritable, IntWritable> {

  private IntWritable result = new IntWritable();

  @Override
  public void reduce(LogWritable key, Iterable<IntWritable> values, Context context)
          throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable val : values) {
      sum += val.get();
    }
    result.set(sum);
    context.write(key, result);
  }
}
