package com.cloudwick.mapreduce.customloginputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by bijay on 12/4/14.
 * This is the input format for log file.
 * It emits ipaddress as key and timestamp, request page and status code as value
 */
public class LogInputFormat extends FileInputFormat<Text, Text> {

  @Override
  /**
   * Returns LogRecordReader
   * @inheritDoc
   */
  public RecordReader<Text, Text> createRecordReader(InputSplit split,
                                                     TaskAttemptContext context) throws IOException, InterruptedException {
    RecordReader<Text, Text> recordReader = (RecordReader<Text, Text>) new LogRecordReader();
    recordReader.initialize(split, context);
    return recordReader;
  }

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    final CompressionCodec codec =
            new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    return (null == codec);
  }
}
