package com.github.elazarl.hadoop.benchmark.mapfile;

import com.google.common.base.Stopwatch;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.*;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Tests performance of creating and searching a plain HBase {@code HFile}.
 */
public class HFileMain {
    private static long percent = 1;

    public static void main(String[] args) throws Exception {
        CommandLine cmd = CmdUtils.parse(args);
        long n = CmdUtils.getN(cmd);
        int iterations = CmdUtils.iterations(cmd);
        Configuration conf = HBaseConfiguration.create();
        benchmark(conf, n, iterations);
    }

    public static void benchmark(Configuration conf, long n, int iterations) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path hfile = new Path("/test/8f11e_SeqId_1_");
        if (fs.exists(hfile)) {
            fs.delete(hfile, true);
        }
        StoreFile.Writer writer = new StoreFile.WriterBuilder(conf, new CacheConfig(conf), fs, 1024 * 100).
                withCompression(Compression.Algorithm.GZ).
                withFilePath(hfile).
                build();
        Stopwatch stopwatch = new Stopwatch().start();
        for (long i = 0; i < n; i++) {
            final KeyValue kv = getKeyValue(i);
            writer.append(kv);
        }
        System.out.println(">>> insertion of " + HBaseMain.format.format(n*iterations) + " took " +
            stopwatch);
        final double ms = stopwatch.elapsedTime(TimeUnit.MILLISECONDS);
        if (ms == 0) {
            System.out.println(">>> Insertion: Cannot measure, too fast");
        } else {
            System.out.println(">>> Insertion: " + HBaseMain.format.format((n/ ms)*1000) + " op/sec");
        }
        writer.close();

        final StoreFile storeFile = new StoreFile(fs, hfile, conf, new CacheConfig(conf), StoreFile.BloomType.NONE,
                null);
        final StoreFile.Reader reader = storeFile.createReader();
        StoreFileScanner scanner = reader.getStoreFileScanner(true, false);
        final long k = (n * percent) / 100;
        Random rand = new Random(0xFA17);
        stopwatch.reset().start();
        for (int j = 0; j < iterations; j++) {
            for (int i = 0; i < k; i++) {
                scanner.seek(getKeyValue(rand.nextInt((int) n)));
                out = scanner.next();
            }
        }
        System.out.println("Searching " + iterations + " times 1% (=" + HBaseMain.format.format(k) + ") took " + stopwatch);
        if (stopwatch.elapsedTime(TimeUnit.MILLISECONDS) == 0) {
            System.out.println(">>> Random Search: too fast to measure");
            return;
        }
        final double opPerSec = (double)(iterations * k) / stopwatch.elapsedTime(TimeUnit.MILLISECONDS)*1000;
        System.out.println(">>> Random Search: " + HBaseMain.format.format(opPerSec) + " op/sec");

    }

    static public KeyValue out;

    private static KeyValue getKeyValue(long i) {
        return new KeyValue(Bytes.toBytes(i), GenerateToHbase.family, null,
                Bytes.toBytes(Pairs.ofIndexString(i)));
    }
}
