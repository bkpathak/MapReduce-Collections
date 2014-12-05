package com.cloudwick.mapreduce.logcustomwritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by bijay on 11/29/14.
 * <p/>
 * The mapper parse the log and emits the combination of (ipAddress, timeStamp,requestPage).
 */
public class LogCustomMapper extends Mapper<LongWritable, Text, LogWritable, IntWritable> {

    private static final IntWritable one = new IntWritable(1);
    private LogWritable logEvents = new LogWritable();

    /**
     * Compile the given regular expression into a pattern
     */
    Pattern pattern = Pattern.compile("((?:\\d{1,3}\\.){3}\\d{1,3}).*" + //ipAddress
            "((?<=\\[).*(?=\\])).*" +                                      //timeStamp
            "((?<=\\]\\s\").*(?=\"\\s+\\d))");                         //requestPage

    Matcher matcher;

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        /**
         * Creates a matcher that will match the given input against this pattern.
         */
        matcher = pattern.matcher(line);
        /**
         * find the next subsequent of the input sequence that matches the pattern.
         */
        if (matcher.find()) {

            /**
             * The group() method returns the input subsequence captured by the given group during
             * the previous match operation.
             */

            logEvents.set(new Text(matcher.group(1)), new Text(matcher.group(2)),
                    new Text(matcher.group(3)));

            context.write(logEvents, one);
        }

    }
}
