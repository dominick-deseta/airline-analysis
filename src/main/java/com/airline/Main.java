package com.airline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;

public class Main extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Main <input path> <output path>");
            return -1;
        }

        String inputPath = args[0];
        String outputPath = args[1];

        // These paths will be used by the Oozie workflow
        String airlineOutputPath = outputPath + "/airline-analysis";
        String taxiOutputPath = outputPath + "/taxi-analysis";
        String cancellationOutputPath = outputPath + "/cancellation-analysis";

        Configuration conf = getConf();
        
        // Job 1: Analyze airline on-time performance
        Job airlineJob = AirlineOnTimeJob.createJob(conf, inputPath, airlineOutputPath);
        boolean success = airlineJob.waitForCompletion(true);
        if (!success) {
            return 1;
        }
        
        // Job 2: Analyze airport taxi times
        Job taxiJob = AirportTaxiTimeJob.createJob(conf, inputPath, taxiOutputPath);
        success = taxiJob.waitForCompletion(true);
        if (!success) {
            return 1;
        }
        
        // Job 3: Analyze flight cancellation reasons
        Job cancellationJob = FlightCancellationJob.createJob(conf, inputPath, cancellationOutputPath);
        success = cancellationJob.waitForCompletion(true);
        if (!success) {
            return 1;
        }
        
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new Main(), args);
        System.exit(exitCode);
    }
}
