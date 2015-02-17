package org.bkpathak.mapreduce.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by bijay on 12/18/14.
 */
public class KeyComparator extends WritableComparator {
  protected KeyComparator() {
    super(CompositeKey.class, true);
  }

  @Override
  public int compare(WritableComparable w1, WritableComparable w2) {
    CompositeKey ck1 = (CompositeKey) w1;
    CompositeKey ck2 = (CompositeKey) w2;
    if (ck1.getIpAddress().compareTo(ck1.getIpAddress()) == 0) {
      return ck1.getTimeStamp().compareTo(ck2.getTimeStamp());
    } else
      return ck1.getTimeStamp().compareTo(ck2.getTimeStamp());

  }
}
