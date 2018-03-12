package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * This is an example Hadoop Map/Reduce application.
 * It reads the text input files, breaks each line into words
 * Map emits <word, docid> with each word emitted once per document
 * Reduce takes map output and emits <word, list(docid)>
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar invertedindex
 *            [-m <i>maps</i>] [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i> 
 *
 * @author Faraz Ahmad
 */
@SuppressWarnings("deprecation")
public class InvertedIndex {

    private enum Counter {

        WORDS, VALUES
    }
    public static final Log LOG = LogFactory.getLog("org.apache.hadoop.examples.InvertedIndex");

    /**
     * For each line of input, break the line into words and emit them as
     * (<b>word,doc</b>).
     */
    public static class MapInveIndex extends Mapper<Object, Text, Text, Text> {

        private String path;

        public void map(Object key, Text value,
                Context context) throws IOException, InterruptedException {

            Path filePath = ((FileSplit) context.getInputSplit()).getPath();
            path = ((FileSplit) context.getInputSplit()).getPath().toString();

            String docName = new String("");
            Text docId, wordText;
            String line;

            StringTokenizer tokens = new StringTokenizer(path, "/");
            while (tokens.hasMoreTokens()) {
                docName = tokens.nextToken();
            }
            docId = new Text(docName);
            line = ((Text) value).toString();
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                wordText = new Text(itr.nextToken());
                if (wordText.toString().length() >= 6 && wordText.toString().matches("^[a-zA-Z]+$")) {
                    context.write(wordText, docId);
                }
            }
        }
    }

    /**
     * The reducer class 
     */
    public static class ReduceInveIndex extends Reducer<Text, Text, Text, Text> {

        String valueString;
        Text docId;

        public void reduce(Text key, Iterable<Text> values,
                Context context,
                Reporter reporter) throws IOException, InterruptedException {

            List<String> duplicateCheck = new ArrayList<>();
            for (Text val : values) {
                valueString = val.toString();
                if (duplicateCheck.contains(valueString)) {
                    continue;
                } else {
                    duplicateCheck.add(valueString);
                    docId = new Text(valueString);
                    context.write(key, docId);
                    reporter.incrCounter(Counter.VALUES, 1);
                }
            }
        }
    }

    static void printUsage() {
        System.out.println("invertedindex [-m <maps>] [-r <reduces>] <input> <output>");
        System.exit(1);
    }

    /**
     * The main driver for map/reduce program.
     * Invoke this method to submit the map/reduce job.
     * @throws IOException When there is communication problems with the 
     *                     job tracker.
     */
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: invertedindex <deadline> <desired number of maps> <in> [<in>...] <out>  <number of reduces");
            System.exit(2);
        }


        Job job = Job.getInstance(conf, "invertedinex Deadline");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(MapInveIndex.class);
        job.setReducerClass(ReduceInveIndex.class);
        job.setCombinerClass(ReduceInveIndex.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);





        job.setDeadline(new Integer(otherArgs[0]));
        job.setDesiredMap(new Integer(otherArgs[1]));
        FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[3]));
        job.setNumReduceTasks(new Integer(otherArgs[4]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
