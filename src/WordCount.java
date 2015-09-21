/**
 * Created by mdautrey on 17/09/15.
 */
import java.io.IOException;
import java.lang.*;
import java.lang.Exception;
import java.lang.InterruptedException;
import java.lang.Iterable;
import java.lang.Override;
import java.lang.String;
import java.lang.System;
import java.util.*;
import java.util.Objects;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import javax.xml.soap.Text;


public class WordCount {
    public static class WordCountMap extends Mapper<Object, Text, Text, IntWritable> {
        @Override public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()){
                String nextToken = tokenizer.nextToken();
                context.write(new Text(nextToken), new IntWritable(1));
            }
        }

    }
    public static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val : values){
                sum += val.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "wordcount");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WordCountMap.class);
        job.setReducerClass(WordCountReduce.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(WordCount.class);
        System.exit(job.waitForCompletion(true)? 0 : 1);
    }
}
