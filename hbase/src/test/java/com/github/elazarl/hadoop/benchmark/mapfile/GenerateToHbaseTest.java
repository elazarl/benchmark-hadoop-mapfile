package com.github.elazarl.hadoop.benchmark.mapfile;

import com.github.elazarl.hadoop.benchmark.mapfile.GenerateToHbase;
import com.github.elazarl.hadoop.benchmark.mapfile.Pairs;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests data genereation and retieval on a mini cluster.
 * Doesn't work now, HBaseTestingUtility probably doesn't work with Hadoop 2...
 */
public class GenerateToHbaseTest {
    private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

    Configuration conf;

    @Before
    public void setUp() throws Exception {
        TEST_UTIL.startMiniCluster();
        conf = TEST_UTIL.getConfiguration();
    }

    @After
    public void tearDown() throws Exception {
        TEST_UTIL.shutdownMiniCluster();
    }

    @Test
    public void testBenchmark() throws Exception {
        final double opSec = HBaseMain.measureOpPerSec(conf, 10000);
        Assert.assertTrue("operations on minicluster absurdly slow", opSec > 100);
    }

    @Test
    public void testWrite() throws Exception {
        final HBaseAdmin admin = new HBaseAdmin(conf);
        final String tableName = "mapfile";
        final HTableDescriptor desc = new HTableDescriptor(tableName);
        desc.addFamily(new HColumnDescriptor(GenerateToHbase.family));
        admin.createTable(desc);
        admin.close();

        final HTable mapw = new HTable(conf, tableName);
        mapw.setAutoFlush(false);
        GenerateToHbase.write(mapw, 100000);
        mapw.close();

        final HTable mapr = new HTable(conf, tableName);
        for (long i = 0; i < 1000; i++) {
            final Result r = mapr.get(new Get(Bytes.toBytes(i)));
            final byte[] value = r.getValue(GenerateToHbase.family, null);
            Assert.assertEquals(Pairs.ofIndexString(i), Bytes.toString(value));
            System.out.println("ix " + Bytes.toString(value));
        }
    }
}
