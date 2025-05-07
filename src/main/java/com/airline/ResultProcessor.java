package com.airline;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ResultProcessor {
    
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: ResultProcessor <airline-output-dir> <taxi-output-dir> <cancellation-output-dir> <final-output-dir>");
            System.exit(1);
        }
        
        String airlineOutputDir = args[0];
        String taxiOutputDir = args[1];
        String cancellationOutputDir = args[2];
        String finalOutputDir = args[3];
        
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        
        // Process airline on-time data
        Map<String, Double> airlineOnTimeMap = readAirlineOnTimeData(fs, airlineOutputDir);
        
        // Process airport taxi time data
        Map<String, Double> airportTaxiMap = readAirportTaxiData(fs, taxiOutputDir);
        
        // Process cancellation reason data
        Map<String, Integer> cancellationMap = readCancellationData(fs, cancellationOutputDir);
        
        // Write final results
        writeFinalResults(fs, finalOutputDir, airlineOnTimeMap, airportTaxiMap, cancellationMap);
        
        System.out.println("Result processing completed successfully!");
    }
    
    private static Map<String, Double> readAirlineOnTimeData(FileSystem fs, String inputDir) throws Exception {
        Map<String, Double> airlineMap = new HashMap<>();
        
        System.out.println("Starting to read airline on-time data from: " + inputDir);
        
        // Read all part files in the output directory
        FileStatus[] files = fs.listStatus(new Path(inputDir));
        for (FileStatus file : files) {
            if (file.getPath().getName().startsWith("part-")) {
                System.out.println("Processing file: " + file.getPath().getName());
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
                String line;
                int lineCount = 0;
                while ((line = reader.readLine()) != null) {
                    lineCount++;
                    System.out.println("Line " + lineCount + ": " + line);
                    try {
                        // Find the last tab or space in the line
                        int lastWhitespace = line.lastIndexOf('\t');
                        if (lastWhitespace == -1) {
                            lastWhitespace = line.lastIndexOf(' ');
                        }
                        
                        if (lastWhitespace > 0) {
                            // Everything before the last whitespace is the airline code
                            String airline = line.substring(0, lastWhitespace).trim();
                            // Everything after the last whitespace is the probability value
                            String valueStr = line.substring(lastWhitespace).trim();
                            
                            try {
                                double probability = Double.parseDouble(valueStr);
                                airlineMap.put(airline, probability);
                                System.out.println("Successfully parsed airline: " + airline + " with probability: " + probability);
                            } catch (NumberFormatException e) {
                                System.err.println("Warning: Could not parse probability value: " + valueStr + " for airline: " + airline);
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Warning: Error processing line: " + line + " - " + e.getMessage());
                    }
                }
                reader.close();
                System.out.println("Processed " + lineCount + " lines from airline file.");
            }
        }
        
        System.out.println("Total airlines collected: " + airlineMap.size());
        return airlineMap;
    }
    
    private static Map<String, Double> readAirportTaxiData(FileSystem fs, String inputDir) throws Exception {
        Map<String, Double> airportMap = new HashMap<>();
        
        System.out.println("Starting to read airport taxi time data from: " + inputDir);
        
        // Read all part files in the output directory
        FileStatus[] files = fs.listStatus(new Path(inputDir));
        for (FileStatus file : files) {
            if (file.getPath().getName().startsWith("part-")) {
                System.out.println("Processing file: " + file.getPath().getName());
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
                String line;
                int lineCount = 0;
                while ((line = reader.readLine()) != null) {
                    lineCount++;
                    try {
                        // Find the last tab or space in the line
                        int lastWhitespace = line.lastIndexOf('\t');
                        if (lastWhitespace == -1) {
                            lastWhitespace = line.lastIndexOf(' ');
                        }
                        
                        if (lastWhitespace > 0) {
                            // Everything before the last whitespace is the airport info
                            String airportInfo = line.substring(0, lastWhitespace).trim();
                            // Everything after the last whitespace is the taxi time
                            String valueStr = line.substring(lastWhitespace).trim();
                            
                            // Only consider total taxi time (both in and out)
                            if (airportInfo.endsWith(",total")) {
                                String airport = airportInfo.substring(0, airportInfo.indexOf(","));
                                try {
                                    double taxiTime = Double.parseDouble(valueStr);
                                    airportMap.put(airport, taxiTime);
                                } catch (NumberFormatException e) {
                                    System.err.println("Warning: Could not parse taxi time: " + valueStr + " for airport: " + airport);
                                }
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Warning: Error processing line: " + line + " - " + e.getMessage());
                    }
                }
                reader.close();
                System.out.println("Processed " + lineCount + " lines from taxi time file.");
            }
        }
        
        System.out.println("Total airports collected: " + airportMap.size());
        return airportMap;
    }
    
    private static Map<String, Integer> readCancellationData(FileSystem fs, String inputDir) throws Exception {
        Map<String, Integer> cancellationMap = new HashMap<>();
        
        System.out.println("Starting to read cancellation data from: " + inputDir);
        
        // Read all part files in the output directory
        FileStatus[] files = fs.listStatus(new Path(inputDir));
        for (FileStatus file : files) {
            if (file.getPath().getName().startsWith("part-")) {
                System.out.println("Processing file: " + file.getPath().getName());
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
                String line;
                int lineCount = 0;
                while ((line = reader.readLine()) != null) {
                    lineCount++;
                    try {
                        // Find the last tab or space in the line
                        int lastWhitespace = line.lastIndexOf('\t');
                        if (lastWhitespace == -1) {
                            lastWhitespace = line.lastIndexOf(' ');
                        }
                        
                        if (lastWhitespace > 0) {
                            // Everything before the last whitespace is the cancellation reason
                            String reason = line.substring(0, lastWhitespace).trim();
                            // Everything after the last whitespace is the count
                            String countStr = line.substring(lastWhitespace).trim();
                            
                            try {
                                int count = Integer.parseInt(countStr);
                                cancellationMap.put(reason, count);
                            } catch (NumberFormatException e) {
                                System.err.println("Warning: Could not parse count: " + countStr + " for reason: " + reason);
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Warning: Error processing line: " + line + " - " + e.getMessage());
                    }
                }
                reader.close();
                System.out.println("Processed " + lineCount + " lines from cancellation file.");
            }
        }
        
        System.out.println("Total cancellation reasons collected: " + cancellationMap.size());
        return cancellationMap;
    }
    
    private static void writeFinalResults(FileSystem fs, String outputDir, 
                                         Map<String, Double> airlineOnTimeMap, 
                                         Map<String, Double> airportTaxiMap, 
                                         Map<String, Integer> cancellationMap) throws Exception {
        // Create output directory if it doesn't exist
        Path outputPath = new Path(outputDir);
        if (!fs.exists(outputPath)) {
            fs.mkdirs(outputPath);
        }
        
        // Write final results to a file
        Path resultFile = new Path(outputDir + "/final-results.txt");
        FSDataOutputStream outputStream = fs.create(resultFile);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
        
        // Process airline on-time data
        writer.write("=== 3 Airlines with Highest and Lowest On-Time Probability ===\n\n");
        
        // Sort airlines by on-time probability
        List<Map.Entry<String, Double>> sortedAirlines = new ArrayList<>(airlineOnTimeMap.entrySet());
        Collections.sort(sortedAirlines, new Comparator<Map.Entry<String, Double>>() {
            @Override
            public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });
        
        // Write airlines with highest on-time probability
        writer.write("Airlines with Highest On-Time Probability:\n");
        int count = 0;
        for (Map.Entry<String, Double> entry : sortedAirlines) {
            if (count < 3) {
                writer.write(String.format("%s: %.4f\n", entry.getKey(), entry.getValue()));
                count++;
            } else {
                break;
            }
        }
        
        writer.write("\nAirlines with Lowest On-Time Probability:\n");
        count = 0;
        for (int i = sortedAirlines.size() - 1; i >= 0; i--) {
            Map.Entry<String, Double> entry = sortedAirlines.get(i);
            if (count < 3) {
                writer.write(String.format("%s: %.4f\n", entry.getKey(), entry.getValue()));
                count++;
            } else {
                break;
            }
        }
        
        // Process airport taxi time data
        writer.write("\n\n=== 3 Airports with Longest and Shortest Average Taxi Time ===\n\n");
        
        // Filter out airports with zero taxi time (no real data)
        Map<String, Double> filteredAirportMap = new HashMap<>();
        for (Map.Entry<String, Double> entry : airportTaxiMap.entrySet()) {
            if (entry.getValue() > 0.01) { // Consider anything above 0.01 minutes as valid data
                filteredAirportMap.put(entry.getKey(), entry.getValue());
            }
        }
        
        // Sort airports by taxi time
        List<Map.Entry<String, Double>> sortedAirports = new ArrayList<>(filteredAirportMap.entrySet());
        Collections.sort(sortedAirports, new Comparator<Map.Entry<String, Double>>() {
            @Override
            public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });
        
        // Write airports with longest taxi time
        writer.write("Airports with Longest Average Taxi Time:\n");
        count = 0;
        for (Map.Entry<String, Double> entry : sortedAirports) {
            if (count < 3) {
                writer.write(String.format("%s: %.2f minutes\n", entry.getKey(), entry.getValue()));
                count++;
            } else {
                break;
            }
        }
        
        writer.write("\nAirports with Shortest Average Taxi Time:\n");
        count = 0;
        for (int i = sortedAirports.size() - 1; i >= 0; i--) {
            Map.Entry<String, Double> entry = sortedAirports.get(i);
            if (count < 3) {
                writer.write(String.format("%s: %.2f minutes\n", entry.getKey(), entry.getValue()));
                count++;
            } else {
                break;
            }
        }
        
        // Process cancellation reason data
        writer.write("\n\n=== Most Common Reason for Flight Cancellations ===\n\n");
        
        // Find most common cancellation reason
        String mostCommonReason = null;
        int maxCount = 0;
        for (Map.Entry<String, Integer> entry : cancellationMap.entrySet()) {
            if (entry.getValue() > maxCount) {
                mostCommonReason = entry.getKey();
                maxCount = entry.getValue();
            }
        }
        
        writer.write(String.format("The most common reason for flight cancellations is: %s (Count: %d)\n", 
                                 mostCommonReason, maxCount));
        
        writer.write("\nAll Cancellation Reasons:\n");
        for (Map.Entry<String, Integer> entry : cancellationMap.entrySet()) {
            writer.write(String.format("%s: %d\n", entry.getKey(), entry.getValue()));
        }
        
        writer.close();
    }
}