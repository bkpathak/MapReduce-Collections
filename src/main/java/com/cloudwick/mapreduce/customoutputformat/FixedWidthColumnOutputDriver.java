package com.cloudwick.mapreduce.customoutputformat;

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
 * Created by bijay on 12/4/14.
 */


public class FixedWidthColumnOutputDriver extends Configured implements Tool {

    @Override
    public int run(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.out.println("Usage: " + this.getClass().getName() + " <in dir> <out dir> \n");
            System.exit(-1);
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf);

        job.setJobName("Fixed width output format");
        job.setJarByClass(FixedWidthColumnOutputDriver.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputFormatClass(FixedWidthColumnOutputFormat.class);

        int ret = job.waitForCompletion(true) ? 0 : 1;
        return ret;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        int exitCode = ToolRunner.run(conf, new FixedWidthColumnOutputDriver(), args);
        System.exit(exitCode);
    }

}


