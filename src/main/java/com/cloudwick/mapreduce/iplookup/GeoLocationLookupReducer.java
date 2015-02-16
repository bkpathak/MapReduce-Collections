package com.cloudwick.mapreduce.iplookup;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by bijay on 12/16/14.
 */
public class GeoLocationLookupReducer
        extends Reducer<Text, IntWritable, Text, LongWritable> {
  private LongWritable locationCount = new LongWritable();

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
          throws IOException, InterruptedException {
    long sum = 0;
    for (IntWritable val : values) {
      sum += val.get();
    }
    locationCount.set(sum);
    context.write(key, locationCount);
  }
}
