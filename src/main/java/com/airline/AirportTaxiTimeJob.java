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

public class AirportTaxiTimeJob {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: AirportTaxiTimeJob <input path> <output path>");
            System.exit(1);
        }
        
        String inputPath = args[0];
        String outputPath = args[1];
        
        Configuration conf = new Configuration();
        Job job = createJob(conf, inputPath, outputPath);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TaxiTimeMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text airportKey = new Text();
        private final Text taxiData = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Skip header line
            if (key.get() == 0 && value.toString().contains("Year,Month")) {
                return;
            }

            String[] fields = value.toString().split(",");
            
            // Ensure the line has enough fields
            if (fields.length < 21) { // Changed from 30 to 21 since TaxiIn is field 19, TaxiOut is field 20
                return;
            }
            
            // Extract origin and destination airports
            String origin = fields[16];
            String dest = fields[17];
            
            // Check if airports are not empty
            if (origin.isEmpty() || dest.isEmpty()) {
                return;
            }
            
            // Flag to track if we found any valid taxi data
            boolean foundValidData = false;
            
            // Process origin airport (taxi out)
            String taxiOutStr = fields.length > 20 ? fields[20] : "NA";
            if (!taxiOutStr.isEmpty() && !taxiOutStr.equals("NA")) {
                double taxiOut = Double.parseDouble(taxiOutStr);
                airportKey.set(origin);
                taxiData.set("out," + taxiOut + ",1");
                context.write(airportKey, taxiData);
                foundValidData = true;
            }
            
            // Process destination airport (taxi in)
            String taxiInStr = fields.length > 19 ? fields[19] : "NA";
            if (!taxiInStr.isEmpty() && !taxiInStr.equals("NA")) {
                double taxiIn = Double.parseDouble(taxiInStr);
                airportKey.set(dest);
                taxiData.set("in," + taxiIn + ",1");
                context.write(airportKey, taxiData);
                foundValidData = true;
            }
            
            // If we didn't find any valid taxi data but still want to count the airports
            if (!foundValidData) {
                // Add some minimal data to ensure we have airport entries, even if no taxi data
                // Use defaults like 0.0 for taxi times
                airportKey.set(origin);
                taxiData.set("default,0.0,1");
                context.write(airportKey, taxiData);
                
                airportKey.set(dest);
                taxiData.set("default,0.0,1");
                context.write(airportKey, taxiData);
            }
        }
    }

    public static class TaxiTimeReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        private final DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalTaxiInTime = 0;
            int taxiInCount = 0;
            double totalTaxiOutTime = 0;
            int taxiOutCount = 0;
            int defaultCount = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                String type = parts[0];
                double time = Double.parseDouble(parts[1]);
                int count = Integer.parseInt(parts[2]);
                
                if (type.equals("in")) {
                    totalTaxiInTime += time * count;
                    taxiInCount += count;
                } else if (type.equals("out")) {
                    totalTaxiOutTime += time * count;
                    taxiOutCount += count;
                } else if (type.equals("default")) {
                    defaultCount += count;
                }
            }
            
            // Calculate average taxi in time
            if (taxiInCount > 0) {
                double avgTaxiIn = totalTaxiInTime / taxiInCount;
                result.set(avgTaxiIn);
                context.write(new Text(key.toString() + ",in"), result);
            }
            
            // Calculate average taxi out time
            if (taxiOutCount > 0) {
                double avgTaxiOut = totalTaxiOutTime / taxiOutCount;
                result.set(avgTaxiOut);
                context.write(new Text(key.toString() + ",out"), result);
            }
            
            // If we have no actual taxi data, output a dummy record so that
            // the airport at least appears in the results
            if (taxiInCount == 0 && taxiOutCount == 0 && defaultCount > 0) {
                result.set(0.0);
                context.write(new Text(key.toString() + ",total"), result);
            } else if (taxiInCount > 0 || taxiOutCount > 0) {
                // Calculate combined average taxi time if we have real data
                int totalCount = taxiInCount + taxiOutCount;
                double avgTaxiTotal = (totalTaxiInTime + totalTaxiOutTime) / totalCount;
                result.set(avgTaxiTotal);
                context.write(new Text(key.toString() + ",total"), result);
            }
        }
    }

    public static Job createJob(Configuration conf, String inputPath, String outputPath) throws IOException {
        Job job = Job.getInstance(conf, "Airport Taxi Time Analysis");
        job.setJarByClass(AirportTaxiTimeJob.class);
        
        job.setMapperClass(TaxiTimeMapper.class);
        job.setReducerClass(TaxiTimeReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        return job;
    }
}
