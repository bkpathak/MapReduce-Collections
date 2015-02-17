package org.bkpathak.mapreduce.secondarysort;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by bijay on 12/18/14.
 */
public class SecondarySortMapper extends Mapper<LongWritable, Text, CompositeKey, NullWritable> {

  Logger logger = LoggerFactory.getLogger(SecondarySortMapper.class);

  private CompositeKey keyValPair = new CompositeKey();
  private static final int NUM_FIELDS = 10;
  public static final Pattern httplogPattern = Pattern.compile("^([\\d.]+) (\\S+) (\\S+)" +
          " \\[(.*)\\] \"([^\\s]+)" +
          " (/[^\\s]*) HTTP/[^\\s]+\" (\\d{3}) (\\d+)" +
          " \"([^\"]+)\" \"([^\"]+)\"$");

  @Override
  public void map(LongWritable key, Text value, Context context)
          throws IOException, InterruptedException {
    Matcher matcher = httplogPattern.matcher(value.toString());

    if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
      logger.warn("Unable to parse input");
    } else {
      keyValPair.set(matcher.group(1), matcher.group(4));
      context.write(keyValPair, NullWritable.get());
    }

  }
}
