package com.sumukh.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

import java.io.IOException;

public class Sort extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Sort(), args);
        System.exit(result);
    }


    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(this.getClass());
        job.setJobName("Sort");
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPaths(job, strings[0]);
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        return job.waitForCompletion(true)? 0 : 1;
    }

    public static class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t");
            String title = parts[0];
            String otherParts = parts[1];

            String[] temp = otherParts.split("###LINKS###");
            String pageRank = temp[0];

            context.write(new DoubleWritable(Double.parseDouble(pageRank) * -1), new Text(title));

        }
    }

    public static class SortReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double actualValue = key.get() * -1;
            String temp;

            for (Text text : values) {
                temp = text.toString();
                context.write(new Text(temp), new DoubleWritable(actualValue));
            }
        }
    }
}
