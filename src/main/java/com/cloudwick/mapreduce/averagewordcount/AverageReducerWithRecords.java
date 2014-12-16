package com.cloudwick.mapreduce.averagewordcount;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;


import java.io.IOException;

/**
 * This class uses "MAP_OUTPUT_RECORDS" for the total word count.
 * It used this total word count for finding average.
 */
public class AverageReducerWithRecords
        extends Reducer<Text, IntWritable, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();
    private Counter mapOutputRecords = null;
    long totalRecords;

    @Override
    protected void setup(Context context) {
        mapOutputRecords = context.getCounter("org.apache.hadoop.mapred.Task$Counter",
                "MAP_OUTPUT_RECORDS");
        totalRecords = mapOutputRecords.getValue();
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set((double) sum / totalRecords);
        context.write(key, result);
    }
}
