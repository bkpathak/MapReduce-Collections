package com.cloudwick.mapreduce.customwritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by bijay on 11/28/14.
 */
public class TextPairMapper extends Mapper<LongWritable, Text, TextPairWritable, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private TextPairWritable textPair = new TextPairWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /*
        *split the input line and set the value to the TextPair class object
        *The input text is split on non-word character.
        */

        String[] words = value.toString().split("\\W+");

        if (words.length == 2) {
            context.write(new TextPairWritable(words[0], words[1]), one);
        }
    }
}
