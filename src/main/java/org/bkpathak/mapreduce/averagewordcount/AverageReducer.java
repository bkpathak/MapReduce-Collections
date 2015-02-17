package org.bkpathak.mapreduce.averagewordcount;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by bijay on 11/26/14.
 * The combiner does the per map average word count of the input text file.
 * This reducer simply prints the result
 */
public class AverageReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

  private Map<Text, Long> keyMap;
  long totalWordCount = 0;
  LongWritable wordCount = new LongWritable();
  Long keyVal = new Long(0l);

  @Override
  protected void setup(Context context) {
    keyMap = new HashMap<Text, Long>();
  }

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
          throws IOException, InterruptedException {
    long sum = 0;
    for (IntWritable val : values) {
      sum += val.get();
    }
    keyVal = keyMap.get(key);
    if (keyVal == null) {
      keyVal = sum;
    } else {
      keyVal = keyVal + sum;
    }
    totalWordCount += sum;
    keyMap.put(new Text(key), keyVal);

  }


  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {

    DoubleWritable result = new DoubleWritable();
    double wordAvg;

    for (Map.Entry<Text, Long> entry : keyMap.entrySet()) {
      wordAvg = (double) entry.getValue() / totalWordCount;
      result.set(wordAvg);
      context.write(entry.getKey(), result);
    }
  }

}
