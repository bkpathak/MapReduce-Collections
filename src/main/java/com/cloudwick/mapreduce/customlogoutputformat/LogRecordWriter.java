package com.cloudwick.mapreduce.customlogoutputformat;


import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by bijay on 12/5/14.
 */
public class LogRecordWriter<K, V> extends RecordWriter<K, V> {

    private DataOutputStream out;

    public LogRecordWriter(DataOutputStream out) {
        this.out = out;
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException {
        out.close();
    }

    @Override
    public void write(K key, V value) throws IOException {
        // format output by separating the key value with =>
        // and surround the value with ()

        String outString = key.toString() + " => " + "( " + value.toString() + ") \n";
        out.writeBytes(outString);
    }
}
