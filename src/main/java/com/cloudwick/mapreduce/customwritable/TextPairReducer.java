package com.cloudwick.mapreduce.customwritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by bijay on 11/28/14.
 */
public class TextPairReducer extends Reducer<TextPairWritable, IntWritable, TextPairWritable, IntWritable> {
    private IntWritable result = new IntWritable();


    public void reduce(TextPairWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
