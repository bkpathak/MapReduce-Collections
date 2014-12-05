package com.cloudwick.mapreduce.logprocessing;

import javassist.bytecode.SyntheticAttribute;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by bijay on 11/27/14.
 */
public class QuickTest {



    public static void main(String[] args) {
        String input = "25.198.250.35 - - [2014-07-19T16:05:33Z] \"GET / HTTP/1.1\" 404 1081 \"-\" \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)\"";
        Pattern pattern = Pattern.compile("((?:\\d{1,3}\\.){3}\\d{1,3}).*" + // + //ipAddress
                "((?<=\\[).*(?=\\])).*" +                                  //timeStamp
                "((?<=\\]\\s\").*(?=\"\\s+\\d))");                         //requestPage

        int[][] array = new int[3][];


        Matcher matcher = pattern.matcher(input);
        matcher.find();
        System.out.println(matcher.group(1));
        System.out.println(matcher.group(2));
        System.out.println(matcher.group(3));



    }
}
