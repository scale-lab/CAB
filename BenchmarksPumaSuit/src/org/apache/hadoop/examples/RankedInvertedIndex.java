package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
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
import org.apache.hadoop.mapred.Reporter;

/**
 * This is an example Hadoop Map/Reduce application.
 * Map reads input data containing each line of the format <word|file n> showing how many times (n)
 * each word appears in each file.
 * Map separates count from the rest of the data in the input. Map output format is <word,<file,n>>. 
 * Reduce emits <word, list<n1, file1>, <n2, file2> ... > in decreasing order of occurrence of the word
 * in the respective files.
 *
 * 
 * To run: bin/hadoop jar build/hadoop-examples.jar rankedinvertedindex
 *            [-m <i>maps</i>] [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i> 
 *
 * @author Faraz Ahmad 
 */
@SuppressWarnings("deprecation")
public class RankedInvertedIndex {

    private enum Counter {

        WORDS, VALUES
    }
    public static final Log LOG = LogFactory.getLog("org.apache.hadoop.examples.RankedInvertedIndex");

    public static class MapRanked extends Mapper<Object, Text, Text, FileCountPair> {

        public void map(Object key, Text value,
                Context context) throws IOException, InterruptedException {

            String wordString = new String();
            String valueString = new String();
            String line = new String();
            String docId = new String();
            int countIndex;
            int count;
            int fileIndex;
            FileCountPair fileCountPair = new FileCountPair();

            line = ((Text) value).toString();

            // extract the count from the input string
            countIndex = line.lastIndexOf("\t");
            valueString = line.substring(0, countIndex);
            count = Integer.parseInt(line.substring(countIndex + 1));

            // extract words and filename from the valueString 
            fileIndex = valueString.lastIndexOf("|");
            wordString = valueString.substring(0, fileIndex);
            docId = valueString.substring(fileIndex + 1);

            fileCountPair = new FileCountPair(docId, count);
            context.write(new Text(wordString), fileCountPair);
        }
    }

    public static class ReduceRanked extends Reducer<Text, FileCountPair, Text, Text> {

        public void reduce(Text key, Iterator<FileCountPair> values,
                Context context,
                Reporter reporter) throws IOException, InterruptedException {

            int count, size;
            String docId = new String("");
            Text pair = new Text();
            FileCountPair valuePair, valueListArr[];
            List<FileCountPair> valueList = new ArrayList<FileCountPair>();

            while (values.hasNext()) {
                valuePair = new FileCountPair((FileCountPair) values.next());
                valueList.add(valuePair);
            }

            size = valueList.size();
            valueListArr = new FileCountPair[size];
            valueList.toArray(valueListArr);
            Arrays.sort(valueListArr);

            for (int i = 0; i < size; i++) {
                count = valueListArr[i].getCount();
                docId = valueListArr[i].getFile();
                pair = new Text(count + "|" + docId);
                context.write(key, pair);
                //reporter.incrCounter(Counter.VALUES, 1);
            }
        }
    }

    static void printUsage() {
        System.out.println("rankedinvertedindex [-m <maps>] [-r <reduces>] <input> <output>");
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
            System.err.println("Usage: rankedinvertedindex <deadline> <desired number of maps> <in> [<in>...] <out>  <number of reduces");
            System.exit(2);
        }


        Job job = Job.getInstance(conf, "rankedinvertedindex Deadline");
        job.setJarByClass(RankedInvertedIndex.class);
        job.setMapperClass(MapRanked.class);
        job.setReducerClass(ReduceRanked.class);
        job.setCombinerClass(ReduceRanked.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FileCountPair.class);
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
