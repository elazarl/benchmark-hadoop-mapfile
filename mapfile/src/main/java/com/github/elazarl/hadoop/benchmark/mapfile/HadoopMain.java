package com.github.elazarl.hadoop.benchmark.mapfile;

import com.google.common.base.Stopwatch;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Usage:
 * <pre>
 *     hadoop jar mapfile-bench-mapfile-jar-with-dependencies.jar -n 10m
 *     # for a single iteration on values:
 *     hadoop jar mapfile-bench-mapfile-jar-with-dependencies.jar -iteration 1 -n 10m
 * </pre>
 */
public class HadoopMain  extends Configured implements Tool {
    DecimalFormat format = new DecimalFormat("#,###.00");
    int iterations = 10;
    private long percent = 1;

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(new Option("help", "display this text"));
        options.addOption(new Option("n", true, "number of lines"));
        options.addOption(new Option("delete", true, "delete previous values"));
        options.addOption(new Option("iteration", true, "number of iterations on known values"));
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse( options, args);
        if (cmd.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "ant", options );
            return 1;
        }
        if (!cmd.hasOption("n")) {
            System.err.println("must choose number of lines (-n)");
            return 2;
        }
        String iterationStr = cmd.getOptionValue("iteration");
        if (iterationStr != null) {
            iterations = Integer.parseInt(iterationStr);
        }
        String nStr = cmd.getOptionValue("n");
        long n = Pairs.shorthandDecimal(nStr);
        final FileSystem fs = FileSystem.newInstance(getConf());
        final Path mapfileDir = new Path("/test/mapfile");
        if (fs.exists(mapfileDir)) {
            FileUtil.fullyDelete(new File(mapfileDir.toString()));
        }
        Stopwatch stopwatch = new Stopwatch().start();
        GenerateMapfile.write(GenerateMapfile.create(getConf(), mapfileDir),
                n);
        System.out.println(">>> Pushing " + format.format(n) + "keyvals of " + Pairs.MIN_LEN + "-" + Pairs.MAX_LEN +
            " took " + stopwatch);
        final double ms = stopwatch.elapsedTime(TimeUnit.MILLISECONDS);
        if (ms == 0) {
            System.out.println(">>> Insertion: Cannot measure, too fast");
        } else {
            System.out.println(">>> Insertion: " + format.format((n/ ms)*1000) + " op/sec");
        }
        stopwatch.reset();


        final MapFile.Reader mapr = new MapFile.Reader(mapfileDir, getConf());
        System.out.println("Measuring lookup of 1% of " + format.format(n));
        LongWritable ixw = new LongWritable();
        Text txt = new Text();
        Random rand = new Random(0xFA17);
        stopwatch.start();
        final long k = (n * percent) / 100;
        for (int j = 0; j < iterations; j++) {
            for (long i = 0; i < n/100; i++) {
                ixw.set(rand.nextInt((int) n));
                mapr.get(ixw, txt);
            }
        }
        stopwatch.stop();
        System.out.println("Searching " + iterations + " times 1% (=" + format.format(k) + ") took " + stopwatch);
        if (stopwatch.elapsedTime(TimeUnit.MILLISECONDS) == 0) {
            System.out.println(">>> Random Search: too fast to measure");
            return -1;
        }
        final double opPerSec = ((double)(iterations * k) / stopwatch.elapsedTime(TimeUnit.MILLISECONDS))*1000;
        System.out.println(">>> Random Search: " + format.format(opPerSec) + " op/sec");
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(
                ToolRunner.run(new Configuration(), new HadoopMain(), args)
        );
    }
}
