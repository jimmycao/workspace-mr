package cn.jimmy.mr.temperature;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature {
  
  static class MaxTemperatureMapper extends 
  Mapper<LongWritable, Text, Text, IntWritable> {
    private static final int MISSING = 9999;
    
    public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException{
      String line = value.toString();
      String year = line.substring(15, 19);
      System.out.println(year);
      int airTemperature;
      if(line.charAt(40) == '+'){
        airTemperature = Integer.parseInt(line.substring(41,45));
      }else{
        airTemperature = Integer.parseInt(line.substring(40,45));
      }
      
      String quality = line.substring(45,46);
      if(airTemperature != MISSING && quality.matches("[01459]")){
        context.write(new Text(year), new IntWritable(airTemperature));
      }
    }
  }
  
  static class MaxTemperatureReducer extends
  Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
    throws IOException, InterruptedException {
      int maxValue = Integer.MIN_VALUE;
      for(IntWritable value : values){
        maxValue = Math.max(maxValue, value.get());
      }
      context.write(key, new IntWritable(maxValue));
    }
  }
  
  public static void main(String[] args) throws Exception {
    if(args.length != 2){
      System.err.println("Usage: MaxTemperature <input path> <output path>");
      System.exit(-1);
    }
    
    Job job = new Job();                  
    job.setJarByClass(MaxTemperature.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(MaxTemperatureMapper.class);
    job.setCombinerClass(MaxTemperatureReducer.class);
    job.setReducerClass(MaxTemperatureReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    System.exit(job.waitForCompletion(true)? 0 : 1);
  }
}