package org.bkpathak.mapreduce.secondarysort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;

/**
 * Created by bijay on 12/18/14.
 */
public class SecondarySortReducer
        extends Reducer<CompositeKey, NullWritable, CompositeKey, NullWritable> {

  @Override
  public void reduce(CompositeKey key, Iterable<NullWritable> values, Context context)
          throws IOException, InterruptedException {
    context.write(key, null);

  }
}
