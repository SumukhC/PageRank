package com.sumukh.pagerank;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LinkGraph extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new LinkGraph(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJobName("LINK GRAPH");
        job.setJarByClass(this.getClass());
        job.setMapperClass(LinkGraphMapper.class);
        job.setReducerClass(LinkGraphReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPaths(job, strings[0]);
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        return job.waitForCompletion(true)? 0 : 1;

    }

    public static class LinkGraphMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Pattern LINK_PATTERN = Pattern.compile("\\[\\[.*?]\\]");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String title;
            String outlink;
            String line = value.toString();
            if (line == null || line.isEmpty()) {
                return;
            }

            title = StringUtils.substringBetween(line, "<title>", "</title>").trim();
            if (title.isEmpty()) {
                return;
            }

            Matcher matcher = LINK_PATTERN.matcher(line);
            while (matcher.find()) {
                outlink = matcher.group().replace("[[", "").replace("]]", "");
                outlink = outlink.trim();

                if (outlink.isEmpty()) {
                    continue;
                }
                context.write(new Text(title), new Text(outlink));
            }

        }
    }


    public static class LinkGraphReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> outlinks = new LinkedList<>();
            for (Text text : values) {
                if (text.toString().isEmpty()) {
                    continue;
                }
                outlinks.add(text.toString());
            }
            context.write(key, new Text(StringUtils.join(new List[]{outlinks})));
        }
    }
}
