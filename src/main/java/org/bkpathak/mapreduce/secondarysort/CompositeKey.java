package org.bkpathak.mapreduce.secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by bijay on 12/18/14.
 */
public class CompositeKey implements WritableComparable<CompositeKey> {

  private Text ipAddress;
  private Text timeStamp;

  public CompositeKey() {
    this.ipAddress = new Text();
    this.timeStamp = new Text();
  }

  public void set(String ipAddress, String timeStamp) {
    this.ipAddress.set(ipAddress);
    this.timeStamp.set(timeStamp);
  }

  /*
  de-serialize the input fields and populate the fields
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    ipAddress.readFields(in);
    timeStamp.readFields(in);
  }

  /*
  write the fields to the output stream
   */
  @Override
  public void write(DataOutput out)
          throws IOException {
    ipAddress.write(out);
    timeStamp.write(out);
  }

  /*
  needs to override comparable to make comparison between keys
   */
  public int compareTo(CompositeKey o) {
    if (ipAddress.compareTo(o.ipAddress) == 0) {
      return timeStamp.compareTo(o.timeStamp);
    } else {
      return ipAddress.compareTo(o.ipAddress);
    }
  }

  @Override
  public int hashCode() {
    return ipAddress.hashCode() + timeStamp.hashCode();
  }

  @Override
  public String toString() {
    return "(" + ipAddress + "\t" + timeStamp + ")";
  }

  public Text getIpAddress() {
    return ipAddress;
  }

  public Text getTimeStamp() {
    return timeStamp;
  }

  public void setIpAddress(Text ipAddress) {
    this.ipAddress = ipAddress;
  }

  public void setTimeStamp(Text timeStamp) {
    this.timeStamp = timeStamp;
  }
}
