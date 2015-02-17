package org.bkpathak.mapreduce.averagewordcount;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by bijay on 12/8/14.
 */
public class QuickTest {

  public static void main(String[] args) {
    Map<String, Long> keyMap = new HashMap<String, Long>();

    keyMap.put("abc", 5l);
    keyMap.put("bcd", 10l);
    keyMap.put("asd", 23l);
    Set<String> keys = keyMap.keySet();

    for (String k : keys) {
      System.out.println(k + "   " + keyMap.get(k));
    }

  }
}
