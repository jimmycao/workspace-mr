package cn.jimmy.mr.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCounter extends Configured  implements Tool{
  
  public static class WordCountMapper extends 
           Mapper<LongWritable, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text word =  new Text();
    
    public void map(LongWritable key, Text value, Context context) 
              throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while(tokenizer.hasMoreElements()){
        word.set(tokenizer.nextToken());
        context.write(word, one);
      }
    }   
  }

  public static class WordCountReducer extends 
             Reducer<Text, IntWritable, Text, IntWritable>{
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
      int sum = 0;
      for(IntWritable val : values){
        sum += val.get();
      }
      context.write(key, new IntWritable(sum));
    }   
  }
  
  public int run(String[] args) throws Exception{
    Job job = new Job(getConf());
    job.setJarByClass(WordCounter.class);
    job.setJobName("WordCount");
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(WordCountMapper.class);
    job.setCombinerClass(WordCountReducer.class);
    job.setReducerClass(WordCountReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    boolean success = job.waitForCompletion(true);
    return success ? 0: 1;
   }
  
  public static void main(String[] args) throws Exception{
    int ret = ToolRunner.run(new WordCounter(), args);
    System.exit(ret);
  }
}