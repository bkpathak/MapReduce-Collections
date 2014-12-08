package com.cloudwick.mapreduce.averagewordcount;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by bijay on 11/26/14.
 */
public class MapperAverageCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable total = new IntWritable();


    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        total.set(sum);
        context.write(key, total);
    }


}
