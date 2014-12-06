package com.cloudwick.mapreduce.customlogoutputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.IOException;

/**
 * Created by bijay on 12/5/14.
 */
public class LogOutputDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.out.println("Usage: " + this.getClass().getName() + " <in dir> <out dir>\n");
            System.exit(-1);
        }
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);

        job.setJobName("Log output format");
        job.setJarByClass(LogOutputDriver.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputFormatClass(LogOutputFormat.class);

        boolean success = job.waitForCompletion(true);
        return (success ? 0 : 1);
    }

    public void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int exitCode = ToolRunner.run(conf, new LogOutputDriver(), args);
        System.exit(exitCode);
    }
}
