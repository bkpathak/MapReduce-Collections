package com.cloudwick.mapreduce.customlogoutputformat;

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
 * Created by bijay on 12/5/14.
 * <p/>
 * Extends the FileOutputFormat. Very similar to TextOutputFormat.
 * Separate the key and value with newline character.
 */
public class LogOutputFormat<K, V> extends FileOutputFormat<K, V> {

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException {
        DataOutputStream out;
        Configuration conf = job.getConfiguration();

        if (getCompressOutput(job)) {
            //if the job output should be compressed, use a compressed output stream
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
            // create the named codec
            CompressionCodec codec = ReflectionUtils.newInstance(codecClass, conf);
            //build the filename including the extension

            Path file = getDefaultWorkFile(job, codec.getDefaultExtension());
            FileSystem fs = file.getFileSystem(conf);
            out = new DataOutputStream(codec.createOutputStream(fs.create(file, false)));
        } else {
            // Otherwise open a stream to the default file for this task
            Path file = getDefaultWorkFile(job, "");
            FileSystem fs = file.getFileSystem(conf);
            out = fs.create(file, false);
        }
        /**
         * create the the custom record reader
         */
        return new LogRecordWriter<K, V>(out);
    }
}
