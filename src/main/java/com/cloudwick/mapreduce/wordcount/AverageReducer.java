package com.cloudwick.mapreduce.wordcount;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by bijay on 11/26/14.
 * The combiner does the per map average word count of the input text file.
 * This reducer simply prints the result
 */
public class AverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        for (DoubleWritable val : values) {
            context.write(key, val);
        }
    }
}
