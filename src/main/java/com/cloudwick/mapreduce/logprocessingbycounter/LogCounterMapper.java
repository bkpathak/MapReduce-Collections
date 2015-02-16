package com.cloudwick.mapreduce.logprocessingbycounter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by bijay on 11/26/14.
 * The hadoop provides user define counter in two ways:
 * Static counter : using enum
 * Dynamic counter : using string
 * <p/>
 * The map function uses dynamic counter, since we need to count the each occurrence of status word which we don't know
 * in advance.
 */
public class LogCounterMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {

  private final static IntWritable one = new IntWritable(1);
  private Text statusCode = new Text();
  static Counter counter = null;

  Pattern pattern = Pattern.compile("(?<=\"\\s)\\d{3}(?=\\s+\\d)");
  Matcher matcher;

  @Override
  public void map(LongWritable key, Text value, Context context)
          throws IOException, InterruptedException {

    String line = value.toString();
    matcher = pattern.matcher(line);
    matcher.find();
    statusCode.set(matcher.group());
    counter = context.getCounter("status counter", statusCode.toString());
    counter.increment(1);
  }

}
