package org.bkpathak.mapreduce.custominputformat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
 * Created by bijay on 11/30/14.
 * <p/>
 * FixedWidthColumnInputFormat is an input format used to read input files
 * which contain fixed length records. The content of a record need not be
 * text.It can be arbitrary binary data. Users must configure the record
 * length property by calling:
 * FixedWidthColumnInputFormat.setRecordLength(conf, recordLength) or
 * conf.setInt(FixedWidthColumnInputFormat.FIXED_RECORD_LENGTH, recordLength);
 */
public class FixedWidthColumnInputFormat extends FileInputFormat<Text, Text> {

  private static final String FIXED_RECORD_LENGTH =
          "fixedcolumnwidthinputformat.record.length";

  /**
   * Set the length of each record
   *
   * @param conf         configuration
   * @param recordLength the length of record
   */

  public static void setRecordLength(Configuration conf, int recordLength) {
    conf.setInt(FIXED_RECORD_LENGTH, recordLength);
  }

  /**
   * Get record length value
   *
   * @param conf configuration
   * @return the record length , 0 means none was set
   */
  public static int getRecordLength(Configuration conf) {
    return conf.getInt(FIXED_RECORD_LENGTH, 0);
  }

  @Override
  public RecordReader<Text, Text>
  createRecordReader(InputSplit split, TaskAttemptContext context)
          throws IOException, InterruptedException {
    int recordLength = getRecordLength(context.getConfiguration());
    if (recordLength <= 0) {
      throw new IOException("Fixed record length   " + recordLength +
              "  is invalid.It should be set to value greater than zero.");
    }
    return new FixedWidthColumnRecordReader();
  }

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    final CompressionCodec codec =
            new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    return (null == codec);
  }

}
