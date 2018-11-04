package com.sumukh.pagerank;

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

public class PageRankAlgorithm extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new PageRankAlgorithm(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(this.getClass());
        job.setJobName("PageRank Algorithm");
        job.setMapperClass(PageRankAlgorithmMapper.class);
        job.setReducerClass(PageRankAlgorithmReducer.class);
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

    public static class PageRankAlgorithmMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t");
            String title = parts[0];
            String[] pageRank = parts[1].split("###LINKS###");
            double pageRankScore = Double.parseDouble(pageRank[0]);
            String[] outlinks = null;
            if (pageRank.length > 1 && !pageRank[1].isEmpty()) {
                outlinks = pageRank[1].split(",");
            }

            context.write(new Text(title), new Text("###SELF###"));

            if (pageRank.length > 1 && !pageRank[1].isEmpty()) {
                double score = pageRankScore / outlinks.length;

                for (String link : outlinks) {
                    context.write(new Text(link), new Text(Double.toString(score)));
                }
            }

            if (pageRank.length > 1 && !pageRank[1].isEmpty()) {
                context.write(new Text(title), new Text("###OUTLINKS###" + pageRank[1]));
            } else {
                context.write(new Text(title), new Text("###OUTLINKS###"));
            }

        }
    }

    public static class PageRankAlgorithmReducer extends Reducer<Text, Text, Text, Text> {

        private double DAMPING_FACTOR = 0.90;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double valueOfC = 0.0;
            double pageRank;
            boolean isSelfPage = false;
            String outlinks = "";

            //Iterate list of values
            for (Text val : values) {
                String v = val.toString().trim();

                //If found, it is a self page and changing the value of the boolean isSelfPage
                if(v.equals("###SELF###")) {
                    isSelfPage = true;
                    continue;
                }

                try {
                    valueOfC += Double.parseDouble(v);
                } catch(NumberFormatException e) {
                    //Get outlinks
                    outlinks = v.split("###OUTLINKS###")[1];
                }
            }

            if(!isSelfPage){
                return;
            }

            //Compute new page rank
            pageRank = (1.0 - DAMPING_FACTOR) + (DAMPING_FACTOR * valueOfC);

            //Emit ‘<Title>, <New Page Rank, List of outgoing link>’
            context.write(new Text(key), new Text(Double.toString(pageRank) + "###LINKS###" + outlinks));
        }
    }
}

