package com.cloudwick.mapreduce.textpair;

import com.cloudwick.mapreduce.customwritable.TextPairWritable;
import com.cloudwick.mapreduce.customwritable.TextPairMapper;
import com.cloudwick.mapreduce.customwritable.TextPairReducer;
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
 * Created by bijay on 11/28/14.
 */
public class TextPairTest {
    MapDriver<LongWritable, Text, TextPairWritable, IntWritable> mapDriver;
    ReduceDriver<TextPairWritable, IntWritable, TextPairWritable, IntWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, TextPairWritable, IntWritable, TextPairWritable, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        TextPairMapper mapper = new TextPairMapper();
        TextPairReducer reducer = new TextPairReducer();

        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("abc   def"))
                .withOutput(new TextPairWritable("abc", "def"), new IntWritable(1))
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));

        reduceDriver.withInput(new TextPairWritable("abc", "bcd"), values)
                .withOutput(new TextPairWritable("abc", "bcd"), new IntWritable(3))
                .runTest();
    }

    @Test

    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text("abc def"))
                .withInput(new LongWritable(), new Text("abcd dffg"))
                .withInput(new LongWritable(), new Text("abc def"))
                .withOutput(new TextPairWritable("abc", "def"), new IntWritable(2))
                .withOutput(new TextPairWritable("abcd", "dffg"), new IntWritable(1))
                .runTest();
    }


}
