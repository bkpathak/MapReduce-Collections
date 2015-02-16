package com.cloudwick.mapreduce.custominputformat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

/**
 * Created by bijay on 11/30/14.
 * A reader to read fixed width column length records from a split.The content of a record need not
 * be text.It can be arbitrary binary data.Users must configure the key and list of values with
 * width of each values and key by calling:
 * FixedWidthColumnRecordReader.setNumValue(conf, numValues) or
 * conf.setInt(FixedWidthColumnRecordReader.FIXED_RECORD_LENGTH, numValue)
 * To set the width of each value will be passed as strings separated by comma:
 * FixedWidthColumnRecordReader.setValuesWidth(conf,String ...valuesWidth )
 * conf.setStrings(FixedWidthColumnRecordReader.FIXED_VALUES_WIDTH, Sting...valuesWidth);
 * <p/>
 * The FIXED_NUM_VALUE should includes list of value present in the record including keys.
 * By default the first value is taken as key.
 */
public class FixedWidthColumnRecordReader extends RecordReader<Text, Text> {

  private static final Log LOG =
          LogFactory.getLog(FixedWidthColumnRecordReader.class);
  private static final String FIXED_NUM_VALUE =
          "fixedwidthcolumnrecordreader.num.values";
  private static final String FIXED_VALUES_WIDTH =
          "fixedwidthcolumnrecordreader.values.width";

  private long start;
  private long pos;
  private long end;

  private FSDataInputStream fileIn;
  private Text key = null;
  private Text value = null;
  private byte[] keybytes = null;
  private byte[] valueList = null;
  private String[] widthString;
  Counter inputByteCounter;


  @Override
  public void initialize(InputSplit genericSplit,
                         TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = context.getConfiguration();
    final Path file = split.getPath();
    this.start = split.getStart();
    this.end = start + split.getLength();
    LOG.info("FixedWidthColumnRecordReader: SPLIT START=" + start + " SPLIT END=" + end +
            " SPLIT LENGTH= " + split.getLength());
    Configuration conf = context.getConfiguration();
    CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
    if (codec != null) {
      throw new IOException("FixedColumnInputFormat doesn't support reading compression class.");
    }

    inputByteCounter = ((MapContext) context).getCounter("FileInputFormatCounters", "BYTES_READ");

    final FileSystem fs = file.getFileSystem(conf);
    fileIn = fs.open(file, (64 * 1024));
    fileIn.seek(start);
    this.pos = start;


    // get the width of value field as array of string
    // initialize the valueList with width of each value width
    // The first item in the value list is for key
    widthString = job.getStrings(FIXED_VALUES_WIDTH);
    int width = 0;
    for (int i = 0; i < job.getInt(FIXED_NUM_VALUE, 0); i++) {
      if (i == 0) {
        keybytes = new byte[Integer.parseInt(widthString[i])];
        continue;
      }
      width += Integer.parseInt(widthString[i]);

    }
    valueList = new byte[width];

  }

  /**
   * Set the number of values to read from each record
   *
   * @param conf     configuration
   * @param numValue number of values in each record
   */
  public static void setNumValue(Configuration conf, int numValue) {
    conf.setInt(FIXED_NUM_VALUE, numValue);
  }

  /**
   * Set the width of each values to to be read from record
   *
   * @param conf        configuration
   * @param valuesWidth width of each values in the record
   */
  public static void setValuesWidth(Configuration conf, String... valuesWidth) {
    conf.setStrings(FIXED_VALUES_WIDTH, valuesWidth);
  }

  @Override
  public boolean nextKeyValue() {
    if (key == null) {
      key = new Text();
    }
    if (value == null) {
      value = new Text();
    }

    // if the current position is less than the split end
    if (pos < end) {
      try {
        fileIn.readFully(pos, keybytes);
        pos += keybytes.length;
        fileIn.readFully(pos, valueList);
        pos += valueList.length;
      } catch (IOException e) {
        key = null;
        value = null;
        return false;
      }

      inputByteCounter.increment(keybytes.length + value.getLength());
      key.set(new Text(keybytes));
      value.set(new Text(valueList));
      return true;
    }
    return false;
  }

  @Override
  public Text getCurrentKey() {
    return key;
  }

  @Override
  public Text getCurrentValue() {
    return value;
  }

  /**
   * rn percentage complete
   */
  @Override
  public float getProgress() {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float) (end - start));
    }
  }

  @Override
  public void close() throws IOException {
    fileIn.close();
  }
}
