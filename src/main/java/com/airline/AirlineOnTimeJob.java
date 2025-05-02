package com.airline;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AirlineOnTimeJob {

    public static class AirlineMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text airlineKey = new Text();
        private final Text flightData = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Skip header line
            if (key.get() == 0 && value.toString().contains("Year,Month")) {
                return;
            }

            String[] fields = value.toString().split(",");
            
            // Ensure the line has enough fields
            if (fields.length < 24) {
                return;
            }
            
            // Extract UniqueCarrier
            String airline = fields[8];
            
            // Check if fields are not empty
            if (airline.isEmpty()) {
                return;
            }
            
            // Check if flight was cancelled or diverted
            String cancelledStr = fields[21];
            String divertedStr = fields[23];
            
            // If these fields are empty, skip this record
            if (cancelledStr.isEmpty() || divertedStr.isEmpty()) {
                return;
            }
            
            // Parse cancelled and diverted flags
            int cancelled = Integer.parseInt(cancelledStr);
            int diverted = Integer.parseInt(divertedStr);
            
            // If flight was cancelled or diverted, it's not on-time
            if (cancelled == 1 || diverted == 1) {
                // Output: airline, "0,1" (not on time, total flights = 1)
                airlineKey.set(airline);
                flightData.set("0,1");
                context.write(airlineKey, flightData);
                return;
            }
            
            // Extract arrival delay
            String arrDelayStr = fields[14];
            
            // If arrival delay field is empty, skip this record
            if (arrDelayStr.isEmpty() || arrDelayStr.equals("NA")) {
                return;
            }
            
            // Parse arrival delay
            double arrDelay = Double.parseDouble(arrDelayStr);
            
            // Flight is on time if arrival delay <= 0
            int onTime = (arrDelay <= 0) ? 1 : 0;
            
            // Output: airline, "onTime,1" (on time status, total flights = 1)
            airlineKey.set(airline);
            flightData.set(onTime + ",1");
            context.write(airlineKey, flightData);
        }
    }

    public static class AirlineReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        private final DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalOnTime = 0;
            int totalFlights = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                totalOnTime += Integer.parseInt(parts[0]);
                totalFlights += Integer.parseInt(parts[1]);
            }

            // Calculate on-time probability
            double onTimeProbability = (double) totalOnTime / totalFlights;
            result.set(onTimeProbability);
            
            context.write(key, result);
        }
    }

    public static Job createJob(Configuration conf, String inputPath, String outputPath) throws IOException {
        Job job = Job.getInstance(conf, "Airline On-Time Analysis");
        job.setJarByClass(AirlineOnTimeJob.class);
        
        job.setMapperClass(AirlineMapper.class);
        job.setReducerClass(AirlineReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        return job;
    }
}
