package com.cloudwick.mapreduce.customloginputformat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

/**
 * Created by bijay on 12/4/14.
 * A reader to read log file.It will parse the log file taking ipaddress as
 */
public class LogRecordReader extends RecordReader<Text, Text> {

    private long start;
    private long pos;
    private long end;
    private FSDataInputStream fileIn;
    private Text key = null;
    private Text value = null;
    private LineReader in;
    private int maxLineLength;
    private CompressionCodecFactory compressionCodecs = null;

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
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
            if(start != 0){
                skipFirstLine = true;
                --start;
                fileIn.seek(start);
            }
            in = new LineReader(fileIn,job);
        }

    }
}
