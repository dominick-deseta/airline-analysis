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
            if (fields.length < 30) {
                return;
            }
            
            // Extract origin and destination airports
            String origin = fields[16];
            String dest = fields[17];
            
            // Check if airports are not empty
            if (origin.isEmpty() || dest.isEmpty()) {
                return;
            }
            
            // Process origin airport (taxi out)
            String taxiOutStr = fields[20];
            if (!taxiOutStr.isEmpty() && !taxiOutStr.equals("NA")) {
                double taxiOut = Double.parseDouble(taxiOutStr);
                airportKey.set(origin);
                taxiData.set("out," + taxiOut + ",1");
                context.write(airportKey, taxiData);
            }
            
            // Process destination airport (taxi in)
            String taxiInStr = fields[19];
            if (!taxiInStr.isEmpty() && !taxiInStr.equals("NA")) {
                double taxiIn = Double.parseDouble(taxiInStr);
                airportKey.set(dest);
                taxiData.set("in," + taxiIn + ",1");
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
            
            // Calculate combined average taxi time
            int totalCount = taxiInCount + taxiOutCount;
            if (totalCount > 0) {
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
