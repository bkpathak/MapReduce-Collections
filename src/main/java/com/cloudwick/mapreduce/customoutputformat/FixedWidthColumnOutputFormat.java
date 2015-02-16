package com.cloudwick.mapreduce.customoutputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;


/**
 * Created by bijay on 12/4/14.
 * This class extends FileOutputFormat.The key and value are shown with their respective width as
 * space is padded to unused byte.
 */
public class FixedWidthColumnOutputFormat<K, V> extends FileOutputFormat<K, V> {

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException {
    DataOutputStream out;
    Configuration conf = job.getConfiguration();

    // Use compressed output stream if job required
    if (getCompressOutput(job)) {
      Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
      // create the codec
      CompressionCodec codec = ReflectionUtils.newInstance(codecClass, conf);
      // build the filename including the extension
      Path file = getDefaultWorkFile(job, codec.getDefaultExtension());
      FileSystem fs = file.getFileSystem(conf);
      out = new DataOutputStream(codec.createOutputStream(fs.create(file, false)));
    } else {
      // otherwise open a stream to the default file for the task
      Path file = getDefaultWorkFile(job, "");
      FileSystem fs = file.getFileSystem(conf);
      out = fs.create(file, false);
    }
    return new FixedWidthColumnRecordWriter<K, V>(out);

  }
}
