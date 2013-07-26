package com.github.elazarl.hadoop.benchmark.mapfile;

import com.github.elazarl.hadoop.benchmark.mapfile.Pairs;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Created by elazar on 7/24/13.
 */
public class PairsTest {
    @Test
    public void testFillOfIndex() throws Exception {
        byte[] first = new byte[Pairs.MAX_LEN], second = new byte[Pairs.MAX_LEN];
        for (int i = 0; i < 100; i++) {
            int firstLen = Pairs.fillOfIndex(i, first);
            int secondLen = Pairs.fillOfIndex(i, second);
            Assert.assertArrayEquals(Arrays.copyOfRange(first, 0, firstLen), Arrays.copyOfRange(second, 0, secondLen));
        }
    }

    @Test
    public void testOfIndexString() throws Exception {
        for (int i = 0; i < 100; i++) {
            String once = Pairs.ofIndexString(i);
            Assert.assertEquals(once, Pairs.ofIndexString(i));
        }
    }
}
