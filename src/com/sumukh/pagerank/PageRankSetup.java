package com.sumukh.pagerank;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class PageRankSetup extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new PageRankSetup(), args);
        System.exit(res);
    }


    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInt("TotalN", getValueOfN(strings[1]));
        Job job = Job.getInstance(configuration);
        job.setJarByClass(this.getClass());
        job.setJobName("PageRank Setup");
        job.setMapperClass(PageRankSetupMapper.class);
        job.setReducerClass(PageRankSetupReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPaths(job, strings[0]);
        FileOutputFormat.setOutputPath(job, new Path(strings[2]));

        return job.waitForCompletion(true)? 0 : 1;
    }

    private int getValueOfN(String string) throws IOException {
        Path path = new Path(string);
        FileSystem fileSystem = path.getFileSystem(new Configuration());
        FileStatus[] statuses = fileSystem.listStatus(path);
        Path[] paths = FileUtil.stat2Paths(statuses);

        String[] contents = null;
        for (Path p : paths) {
            if (fileSystem.isFile(p)) {
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(p)));
                String line = bufferedReader.readLine();
                if (line != null && !line.isEmpty()) {
                    if (line.contains("TotalN")) {
                        contents = line.split("\t");
                        bufferedReader.close();
                        break;
                    }
                }
            }
        }
        if (contents == null) {
            throw new IOException("Contents not initialized!");
        }
        fileSystem.close();
        return Integer.parseInt(contents[1]);
    }

    public static class PageRankSetupMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String inputLine = value.toString();
            String[] parts = inputLine.split("\t");
            String title = parts[0];
            String remString = StringUtils.replace(parts[1], "[", "").replace("]", "").trim();
            String outlinks = "";
            if (parts.length == 2) {
                outlinks = remString;
            }
            context.write(new Text(title), new Text(outlinks));
        }
    }

    public static class PageRankSetupReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalSizeN = context.getConfiguration().getInt("TotalN", 1);
            double initialPageRank = 1.0 / totalSizeN;
            for (Text text : values) {
                String links = text.toString().replace(" ", "");
                context.write(new Text(key), new Text(Double.toString(initialPageRank) + "###LINKS###" + links));
            }
        }
    }
}
