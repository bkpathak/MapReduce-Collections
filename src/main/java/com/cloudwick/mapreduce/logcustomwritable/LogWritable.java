package com.cloudwick.mapreduce.logcustomwritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import sun.rmi.runtime.Log;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by bijay on 11/28/14.
 */
public class LogWritable implements WritableComparable<LogWritable> {

    private Text ipAddress;
    private Text timeStamp;
    private Text requestPage;

    /**
     * Empty constructor - required for serialization and used by mapreduce  framework for object instantiate.
     */

    public LogWritable() {

        set(new Text(), new Text(), new Text());
    }

    public LogWritable(String ipAddress, String timeStamp, String requestPage) {
        set(new Text(ipAddress), new Text(timeStamp), new Text(requestPage));
    }

    public LogWritable(Text ipAddress, Text timeStamp, Text requestPage) {

        set(ipAddress, timeStamp, requestPage);
    }

    public void set(Text ipAddress, Text timeStamp, Text requestPage) {
        this.ipAddress = ipAddress;
        this.timeStamp = timeStamp;
        this.requestPage = requestPage;
    }

    public Text getTimeStamp() {
        return timeStamp;
    }

    public Text getIpAddress() {
        return ipAddress;
    }

    public Text getRequestPage() {
        return requestPage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LogWritable)) return false;

        LogWritable that = (LogWritable) o;

        if (!ipAddress.equals(that.ipAddress)) return false;
        if (!requestPage.equals(that.requestPage)) return false;
        if (!timeStamp.equals(that.timeStamp)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = ipAddress.hashCode();
        result = 31 * result + timeStamp.hashCode();
        result = 31 * result + requestPage.hashCode();
        return result;
    }

    @Override
    public int compareTo(LogWritable lw) {
        int cmp1 = this.ipAddress.compareTo(lw.ipAddress);
        int cmp2 = this.timeStamp.compareTo(lw.timeStamp);
        int cmp3 = this.requestPage.compareTo(lw.requestPage);
        if (cmp1 != 0) {
            return (cmp1 < 0 ? -1 : 1);
        } else if (cmp2 != 0) {
            return (cmp2 < 0 ? -1 : 1);
        } else if (cmp3 != 0) {
            return (cmp3 != 0 ? -1 : 1);
        }
        return 0;
    }

    @Override
    public String toString() {
        return "(" + this.ipAddress + ", " + this.timeStamp +
                ", " + this.requestPage + ")";
    }


    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ipAddress.readFields(dataInput);
        timeStamp.readFields(dataInput);
        requestPage.readFields(dataInput);


    }

    @Override

    public void write(DataOutput out) throws IOException {
        ipAddress.write(out);
        timeStamp.write(out);
        requestPage.write(out);

    }


}
