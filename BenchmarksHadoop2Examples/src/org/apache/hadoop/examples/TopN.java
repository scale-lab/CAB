/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.examples;

/**
 *
 * @author Morteza
 */
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopN {

    public static class MapTopN
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                if (word.toString().length() >= 5 && word.toString().matches("^[a-zA-Z]+$")) {
                    context.write(word, one);
                }

            }
        }
    }

    public static class CombinerTopN
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(new Text(key), new IntWritable(sum));
        }
    }

    public static class ReducerTopN
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();
        private HashMap<String, Integer> countMap = new HashMap<>();

        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            countMap.put(key.toString(), sum);
        }

        private static HashMap sortByValues(HashMap map) {
            List list = new LinkedList(map.entrySet());
            // Defined Custom Comparator here
            Collections.sort(list, new Comparator() {

                public int compare(Object o1, Object o2) {
                    return ((Comparable) ((Map.Entry) (o1)).getValue()).compareTo(((Map.Entry) (o2)).getValue());
                }
            });

            // Here I am copying the sorted list in HashMap
            // using LinkedHashMap to preserve the insertion order
            HashMap sortedHashMap = new LinkedHashMap();
            for (Iterator it = list.iterator(); it.hasNext();) {
                Map.Entry entry = (Map.Entry) it.next();
                sortedHashMap.put(entry.getKey(), entry.getValue());
            }
            return sortedHashMap;
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> sortedMap = sortByValues(countMap);
            Scanner scanner = new Scanner(new File("/usr/local/hadoop/Nword.txt"));
            int Nword = scanner.nextInt();
            int counter = 0;
            int TotalNumber = sortedMap.size();
            context.write(new Text("size of HashMap"), new IntWritable(TotalNumber));
            for (String key : sortedMap.keySet()) {
                if (counter++ <= TotalNumber - Nword) {
                    continue;
                }
                context.write(new Text(key), new IntWritable(sortedMap.get(key)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: topN <deadline> <desired number of maps> <in> [<in>...] <out> ");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "topN");
        job.setJarByClass(TopN.class);
        job.setMapperClass(MapTopN.class);
        job.setNumReduceTasks(1);
        job.setCombinerClass(CombinerTopN.class);
        job.setReducerClass(ReducerTopN.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);




        job.setDeadline(new Integer(otherArgs[0]));
        job.setDesiredMap(new Integer(otherArgs[1]));
        FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
