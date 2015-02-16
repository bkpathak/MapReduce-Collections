package com.cloudwick.mapreduce.averagewordcount;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.DoubleBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bijay on 12/8/14.
 */
public class AveragePerMapperTesting {

  MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

  ReduceDriver<Text, IntWritable, Text, IntWritable> combinerDriver;
  MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable> mapReduceDriver;

  @Before

  public void setUp() {
    TokenizerMapper mapper = new TokenizerMapper();
    AverageReducer reducer = new AverageReducer();
    MapperAverageCombiner combiner = new MapperAverageCombiner();
    mapDriver = MapDriver.newMapDriver(mapper);
    combinerDriver = ReduceDriver.newReduceDriver(combiner);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

  }

  @Test
  public void testMapper() throws IOException {
    mapDriver.withInput(new LongWritable(), new Text("abc\tbcd"))
            .withOutput(new Text("abc"), new IntWritable(1))
            .withOutput(new Text("bcd"), new IntWritable(1))
            .runTest();
  }

  @Test
  public void testCombiner() throws IOException {
    List<IntWritable> values = new ArrayList<IntWritable>();
    values.add(new IntWritable(1));
    values.add(new IntWritable(1));

    combinerDriver.withInput(new Text("abc"), values)
            .withOutput(new Text("abc"), new IntWritable(2))
            .runTest();
  }
}
