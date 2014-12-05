package com.cloudwick.mapreduce.logprocessingbycounter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by bijay on 11/28/14.
 */
public class LogProcessCounterDriver extends Configured implements Tool {

    @Override

    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 1) {
            System.err.println("Usage: LogProcessCounterDriver <in> <out>");
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf);

        job.setJobName("Log processing by counter");
        job.setJarByClass(LogProcessCounterDriver.class);
        job.setMapperClass(LogCounterMapper.class);

        job.setNumReduceTasks(0);
        job.setOutputFormatClass(NullOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        int ret = job.waitForCompletion(true) ? 0 : 1;
        return ret;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new LogProcessCounterDriver(), args);
    }
}
