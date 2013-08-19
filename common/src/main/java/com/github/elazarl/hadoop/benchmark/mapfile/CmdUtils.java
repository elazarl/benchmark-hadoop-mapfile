package com.github.elazarl.hadoop.benchmark.mapfile;

import org.apache.commons.cli.*;

/**
 * Utilities to aid command line parsing for the benchmark.
 */
public class CmdUtils {
    static CommandLine parse(String[] args, Option... opts) throws ParseException {
        Options defaults = defaultOptions();
        for (Option option : opts) {
            defaults.addOption(option);
        }
        return getCommandLine(args, defaults);
    }

    static Options defaultOptions() {
        Options options = new Options();
        options.addOption(new Option("help", "display this text"));
        options.addOption(new Option("n", true, "number of lines"));
        options.addOption(new Option("iteration", true, "number of iterations on known values"));
        return options;
    }

    static CommandLine getCommandLine(String[] args, Options options) throws ParseException {
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
        return cmd;
    }

    public static int iterations(CommandLine cmd) {
        return Integer.parseInt(cmd.getOptionValue("iterations", "10"));
    }

    public static long getN(CommandLine cmd) {
        return Pairs.shorthandDecimal(cmd.getOptionValue("n"));
    }
}
