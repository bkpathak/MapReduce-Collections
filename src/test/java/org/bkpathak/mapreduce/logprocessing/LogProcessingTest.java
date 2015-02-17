package org.bkpathak.mapreduce.logprocessing;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bijay on 11/27/14.
 */
public class LogProcessingTest {

  MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
  ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

  @Before
  public void setUp() {
    LogProcessMapper mapper = new LogProcessMapper();
    LogStatusReducer reducer = new LogStatusReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }

  @Test
  public void testMapper() throws IOException {
    mapDriver.withInput(new LongWritable(), new Text("25.198.250.35 - - [2014-07-19T16:05:33Z] \"GET / HTTP/1.1\" " +
            "404 1081 \"-\" \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)\""))
            .withOutput(new Text("404"), new IntWritable(1))
            .runTest();
  }

  @Test
  public void testReducer() throws IOException {
    List<IntWritable> values = new ArrayList<IntWritable>();
    values.add(new IntWritable(1));
    values.add(new IntWritable(1));
    values.add(new IntWritable(1));

    reduceDriver.withInput(new Text("404"), values)
            .withOutput(new Text("404"), new IntWritable(3))
            .runTest();
  }

  @Test
  public void testMapReduce() throws IOException {
    mapReduceDriver.withInput(new LongWritable(), new Text("25.198.250.35 - - [2014-07-19T16:05:33Z] \"GET / " +
            "HTTP/1.1\" 404 1081 \"-\" \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)\"\n"))
            .withInput(new LongWritable(), new Text("197.188.107.201 - - [2014-07-19T16:05:33Z] \"GET / " +
                    "HTTP/1.1\" 503 1119 \"-\" \"Mozilla/5.0 (X11; Linux x86_64; rv:6.0a1) Gecko/20110421 Firefox/6.0a1\"\n"))
            .withOutput(new Text("404"), new IntWritable(1))
            .withOutput(new Text("503"), new IntWritable(1))
            .runTest();
  }

}
