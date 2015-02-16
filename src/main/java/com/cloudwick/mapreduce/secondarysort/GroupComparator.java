package com.cloudwick.mapreduce.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by bijay on 12/18/14.
 */
public class GroupComparator extends WritableComparator {
  protected GroupComparator() {
    super(CompositeKey.class, true);
  }

  @Override
  public int compare(WritableComparable w1, WritableComparable w2) {
    CompositeKey ck1 = (CompositeKey) w1;
    CompositeKey ck2 = (CompositeKey) w2;

    return ck1.getIpAddress().compareTo(ck2.getIpAddress());
  }

}
