package com.sumukh.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.nio.file.FileSystems;

public class PageRankDriver {

    public static void main(String[] args) throws Exception {
        String inputDirectory = args[0];
        String outputDirectory = args[1];

        java.nio.file.Path path = FileSystems.getDefault().getPath(".");
        String linkGraph = path.toString() + "/LinkGraphOutputDirectory";
        String lineCounter = path.toString() + "/LineNCounterOutputDirectory";
        String pageRankSetup = path.toString() + "/PageRankSetupOutputDirectory";
        String pageRankAlgorithm = path.toString() + "/PageRankAlgorithmOutDirectory";

        ToolRunner.run(new LinkGraph(), new String[] {inputDirectory, linkGraph});
        ToolRunner.run(new LineNCounter(), new String[] {inputDirectory, lineCounter});
        ToolRunner.run(new PageRankSetup(), new String[] {linkGraph, lineCounter, pageRankSetup});
        int i;
        for (i = 1; i <= 10; i++) {
            String iterationInputDirectory;
            if (1 == i) {
                iterationInputDirectory = pageRankSetup;
            } else {
                iterationInputDirectory = pageRankAlgorithm + "/iteration_" + Integer.toString(i - 1);
            }

            String iterationOutputDirectory = pageRankAlgorithm + "/iteration_" + Integer.toString(i);

            ToolRunner.run(new PageRankAlgorithm(), new String[] {iterationInputDirectory, iterationOutputDirectory});

            clear(new Path(iterationInputDirectory));

        }

        String input = pageRankAlgorithm + "/iteration_" + Integer.toString(i - 1);
        ToolRunner.run(new Sort(), new String[] {input, outputDirectory});
    }

    private static void clear(Path path) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.delete(path);
    }
}
