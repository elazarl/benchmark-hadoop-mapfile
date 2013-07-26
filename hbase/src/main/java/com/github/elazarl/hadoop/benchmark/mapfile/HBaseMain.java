package com.github.elazarl.hadoop.benchmark.mapfile;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Usage:
 * Run
 * <pre>
 *     java -cp `hbase classpath`:mapfile-bench-hbase.jar com.github.elazarl.hadoop.benchmark.mapfile.HBaseMain \
 *         -n 1m -bucket 1000
 * </pre>
 */
public class HBaseMain {
    public static int BUCKET_SIZE = 1000;
    public static byte[] value;
    static long percent = 1;
    private static int iterations = 10;

    public static double measureOpPerSec(Configuration conf, long n) throws IOException {
        final String tableName = "benchmark_mapfile";
        prepareTestTable(tableName, conf);
        writeDataToTestTable(n, tableName, new HTable(conf, tableName));

        System.out.println("Opening " + tableName);
        Stopwatch stopwatch = new Stopwatch().start();
        final HTable mapr = new HTable(conf, tableName);
        final long k = (n * percent) / 100;
        List<Get> gets = Lists.newArrayList();
        Random rand = new Random(0xFA17);
        for (int j = 0; j < iterations; j++) {
            for (long i = 0; i < k; i+=BUCKET_SIZE) {
                gets.clear();
                for (int bucket = 0; bucket < BUCKET_SIZE; bucket++) {
                    gets.add(new Get(Bytes.toBytes(rand.nextInt((int) n))));
                }
                final Result[] r = mapr.get(gets);
                int add = 0;
                for (Result result : r) {
                    value = result.getValue(GenerateToHbase.family, null);
                }
            }
        }
        stopwatch.stop();
        System.out.println("Searching " + iterations + " times 1% (=" + format.format(k) + ") took " + stopwatch);
        if (stopwatch.elapsedTime(TimeUnit.MILLISECONDS) == 0) {
            System.out.println(">>> Random Search: too fast to measure");
            return -1;
        }
        final double opPerSec = (double)(iterations * k) / stopwatch.elapsedTime(TimeUnit.MILLISECONDS)*1000;
        System.out.println(">>> Random Search: " + format.format(opPerSec) + " op/sec");
        return opPerSec;
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(new Option("help", "display this text"));
        options.addOption(new Option("n", true, "number of lines"));
        options.addOption(new Option("delete", true, "delete previous values"));
        options.addOption(new Option("bucket", true, "size of the bucket which will be sent to HBase"));
        options.addOption(new Option("iteration", true, "number of iterations on known values"));
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse( options, args);
        if (cmd.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "ant", options );
            System.exit(1);
        }
        if (!cmd.hasOption("n")) {
            System.err.println("must choose number of lines (-n)");
            System.exit(2);
        }
        final String iterationStr = cmd.getOptionValue("iteration");
        if (iterationStr != null) {
            iterations = Integer.parseInt(iterationStr);
        }
        final String bucket = cmd.getOptionValue("bucket");
        if (bucket != null) {
            BUCKET_SIZE = Integer.parseInt(bucket);
        }

        final Configuration conf = HBaseConfiguration.create();
        System.out.println("Getting admin " + conf.get("hbase.zookeeper.quorum"));
        System.out.println(conf.getResource("hbase-site.xml"));
        measureOpPerSec(conf, Pairs.shorthandDecimal(cmd.getOptionValue("n")));
    }

    static final DecimalFormat format = new DecimalFormat("#,###.00");

    private static void writeDataToTestTable(long n, String tableName, HTable hTable) throws IOException {
        System.out.println("Opening " + tableName);
        HTable table = hTable;
        table.setAutoFlush(false);
        System.out.println("Writing " + tableName);
        Stopwatch stopwatch = new Stopwatch().start();
        GenerateToHbase.write(table, n);
        System.out.println("Closing " + tableName);
        table.close();
        stopwatch.stop();
        System.out.println("Writing " + format.format(n) + " took " + stopwatch);
        final double ms = stopwatch.elapsedTime(TimeUnit.MILLISECONDS);
        if (ms == 0) {
            System.out.println(">>> Insertion: Cannot measure, too fast");
        } else {
            System.out.println(">>> Insertion: " + format.format((n/ ms)*1000) + " op/sec");
        }
    }

    private static void prepareTestTable(String tableName, Configuration conf) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)) {
            System.out.println("Disabling " + tableName);
            admin.disableTable(tableName);
            System.out.println("Deleting " + tableName);
            admin.deleteTable(tableName);
        }
        System.out.println("Creating " + tableName);
        final HTableDescriptor desc = new HTableDescriptor(tableName);
        desc.addFamily(new HColumnDescriptor(GenerateToHbase.family));
        admin.createTable(desc);
        System.out.println("Closing " + tableName);
        admin.close();
    }

}
