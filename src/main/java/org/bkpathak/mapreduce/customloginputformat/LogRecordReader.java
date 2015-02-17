package org.bkpathak.mapreduce.customloginputformat;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by bijay on 12/4/14.
 * A reader to read log file.
 */
public class LogRecordReader extends RecordReader<Text, Text> {

  private long start;
  private long pos;
  private long end;
  private FSDataInputStream fileIn;
  private Text key = null;
  private Text record = null;
  private Text value = null;
  private LineReader in;
  private int maxLineLength;
  private CompressionCodecFactory compressionCodecs = null;
  private static final Log LOG = LogFactory.getLog(LineRecordReader.class);

  /**
   * Compile the given regular expression into a pattern
   */
  Pattern pattern = Pattern.compile("((?:\\d{1,3}\\.){3}\\d{1,3}).*" + //ipAddress
          "((?<=\\[).*(?=\\])).*" +                                      //timeStamp
          "((?<=\\]\\s\").*(?=\"\\s+\\d))");                         //requestPage

  Matcher matcher;

  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = context.getConfiguration();
    this.maxLineLength = job.getInt("mapreduce.input.linerecordreader.line.maxlength", Integer.MAX_VALUE);
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();
    compressionCodecs = new CompressionCodecFactory(job);
    final CompressionCodec codec = compressionCodecs.getCodec(file);

    //open the file and seek to the start of the split

    FileSystem fs = file.getFileSystem(job);
    FSDataInputStream fileIn = fs.open(split.getPath());
    boolean skipFirstLine = false;
    if (codec != null) {
      in = new LineReader(codec.createInputStream(fileIn), job);
      end = Long.MAX_VALUE;
    } else {
      if (start != 0) {
        skipFirstLine = true;
        --start;
        fileIn.seek(start);
      }
      in = new LineReader(fileIn, job);
    }
    if (skipFirstLine) {
      //skip the first line and re-establish "start".
      start += in.readLine(new Text(), 0, (int) Math.min((long) Integer.MAX_VALUE, end - start));
    }
    this.pos = start;
    record = new Text();
  }

  public boolean nextKeyValue() throws IOException {
    if (key == null) {
      key = new Text();
    }
    if (value == null) {
      value = new Text();
    }
    int newSize = 0;
    while (pos < end) {
      newSize = in.readLine(record, maxLineLength,
              Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
      if (newSize == 0) {
        break;
      }
      pos += newSize;
      if (newSize < maxLineLength) {
        break;
      }
      LOG.info("Skipped line of size " + " at pos " +
              (pos - newSize));
    }

    String line = record.toString();
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
      key.set(new Text(matcher.group(1)));
      value.set(new Text(matcher.group(2) + matcher.group(3)));
    }
    if (newSize == 0) {
      key = null;
      value = null;
      record = null;
      return false;
    } else {
      return true;
    }
  }

  @Override
  public Text getCurrentKey() {
    return key;
  }

  @Override
  public Text getCurrentValue() {
    return value;
  }

  public float getProgress() {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float) (end - start));
    }
  }

  public void close() throws IOException {
    if (in != null) {
      in.close();
    }
  }

}
