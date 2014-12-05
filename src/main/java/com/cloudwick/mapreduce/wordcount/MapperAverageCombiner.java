package com.cloudwick.mapreduce.wordcount;

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
public class MapperAverageCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private int wordCount = 0;
    private DoubleWritable average = new DoubleWritable();
    private Map<Text, Double> wordMap = new HashMap<>();

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) {
        double sum = 0;
        for (DoubleWritable val : values) {
            sum += val.get();
        }
        wordCount += (int) sum;
        Double mapValue = wordMap.get(key);
        if (mapValue == null) {
            wordMap.put(key, sum);
        } else {
            wordMap.put(key, mapValue + sum);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {

        for (Map.Entry<Text, Double> keyValueEntry : wordMap.entrySet()) {
            average.set(keyValueEntry.getValue() / wordCount);
            context.write(keyValueEntry.getKey(), average);
        }
    }
}
