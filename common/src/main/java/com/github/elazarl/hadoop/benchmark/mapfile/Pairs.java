package com.github.elazarl.hadoop.benchmark.mapfile;

import java.nio.charset.Charset;
import java.util.Random;

/**
 * {@code Pairs} generate pairs of integer -> string, where one can generate the string from only the integer.
 */
public class Pairs {
    public static final int MAX_LEN = 100;
    public static final int MIN_LEN = 10;

    public static int fillOfIndex(long ix, byte[] out) {
        Random random = new Random(ix);
        int len = random.nextInt(MAX_LEN - MIN_LEN) + MIN_LEN;
        for (int i = 0; i < len; i++) {
            out[i] = (byte) (random.nextInt(22) + 'a');
        }
        return len;
    }

    public static String ofIndexString(long ix) {
        byte b[] = new byte[MAX_LEN];
        int to = fillOfIndex(ix, b);
        return new String(b, 0, to, Charset.forName("ASCII"));
    }

    public static long shorthandDecimal(String nStr) {
        long n = 1;
        if (nStr.endsWith("m")) {
            nStr = nStr.substring(0, nStr.length()-1);
            n = 1000000L;
        }
        if (nStr.endsWith("k")) {
            nStr = nStr.substring(0, nStr.length()-1);
            n = 1000L;
        }
        n *= Long.parseLong(nStr);
        return n;
    }
}
