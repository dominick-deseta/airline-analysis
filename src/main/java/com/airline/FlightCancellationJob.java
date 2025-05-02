package com.airline;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlightCancellationJob {

    public static class CancellationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final Text cancellationReason = new Text();
        private final IntWritable one = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Skip header line
            if (key.get() == 0 && value.toString().contains("Year,Month")) {
                return;
            }

            String[] fields = value.toString().split(",");
            
            // Ensure the line has enough fields
            if (fields.length < 23) {
                return;
            }
            
            // Extract cancelled flag and cancellation code
            String cancelledStr = fields[21];
            String cancellationCode = fields[22];
            
            // Check if flight was cancelled
            if (cancelledStr.isEmpty() || !cancelledStr.equals("1")) {
                return;
            }
            
            // If cancellation code is empty or NA, skip this record
            if (cancellationCode.isEmpty() || cancellationCode.equals("NA")) {
                return;
            }
            
            // Map cancellation codes to reasons
            String reason;
            switch (cancellationCode) {
                case "A":
                    reason = "Carrier";
                    break;
                case "B":
                    reason = "Weather";
                    break;
                case "C":
                    reason = "National Air System";
                    break;
                case "D":
                    reason = "Security";
                    break;
                default:
                    reason = "Unknown";
                    break;
            }
            
            cancellationReason.set(reason);
            context.write(cancellationReason, one);
        }
    }

    public static class CancellationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static Job createJob(Configuration conf, String inputPath, String outputPath) throws IOException {
        Job job = Job.getInstance(conf, "Flight Cancellation Analysis");
        job.setJarByClass(FlightCancellationJob.class);
        
        job.setMapperClass(CancellationMapper.class);
        job.setReducerClass(CancellationReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        return job;
    }
}
