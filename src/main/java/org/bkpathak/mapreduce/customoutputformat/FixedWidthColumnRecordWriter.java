package org.bkpathak.mapreduce.customoutputformat;


import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;


/**
 * Created by bijay on 12/4/14.
 * <p/>
 * Write out a record. function are similar to TextOutputFormat.
 * It writes output with their width as specified.
 */
public class FixedWidthColumnRecordWriter<K, V> extends RecordWriter<K, V> {

  private DataOutputStream out;

  public FixedWidthColumnRecordWriter(DataOutputStream out) {
    this.out = out;
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException {
    out.close();

  }

  @Override
  public void write(K key, V value) throws IOException {
    // Creates the key value pair
    String outstring = String.format("%s%s\n", key.toString(), value.toString());
    out.writeBytes(outstring);
  }
}
