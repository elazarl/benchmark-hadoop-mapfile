package com.github.elazarl.hadoop.benchmark.mapfile;

import com.github.elazarl.hadoop.benchmark.mapfile.GenerateMapfile;
import com.github.elazarl.hadoop.benchmark.mapfile.Pairs;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by elazar on 7/24/13.
 */
public class GenerateMapfileTest {
    MiniDFSCluster cluster;
    Configuration conf;

    @Before
    public void setUp() throws IOException {
        cluster = new MiniDFSCluster.Builder(new Configuration()).build();
        conf = cluster.getConfiguration(0);
    }

    @After
    public void tearDown() {
        cluster.shutdown();
    }

    @Test
    public void testGenerator() throws IOException {
        Path dirName = new Path("/", "mapfile");
        MapFile.Writer mapw = GenerateMapfile.create(conf, dirName);
        GenerateMapfile.write(mapw, 1000);
        mapw.close();
        MapFile.Reader mapr = new MapFile.Reader(dirName, conf);
        LongWritable ixw = new LongWritable();
        Text r = new Text();
        for (int i = 20; i < 100; i++) {
            ixw.set(i);
            mapr.get(ixw, r);
            Assert.assertEquals(Pairs.ofIndexString(i), r.toString());
        }
    }
}
