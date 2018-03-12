package org.apache.hadoop.examples;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Charsets;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordMedian {

    private final static IntWritable ONE = new IntWritable(1);
    /**
     * Maps words from line of text into a key-value pair; the length of the word
     * as the key, and 1 as the value.
     */
    private IntWritable length = new IntWritable();

    public static class WordMedianMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                if (word.toString().length() >= 5) {
                    context.write(word, ONE);
                }

            }
        }
    }

//        public static class CombinerTopN
//                extends Reducer<Text, IntWritable, Text, IntWritable> {
//
//            public void reduce(Text key, Iterable<IntWritable> values,
//                    Context context) throws IOException, InterruptedException {
//                int sum = 0;
//                for (IntWritable val : values) {
//                    sum += val.get();
//                }
//                context.write(new Text(key), new IntWritable(sum));
//            }
//        }
    public static class ReducerMedian
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();
        private HashMap<Text, IntWritable> countMap = new HashMap<>();

        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            countMap.put(new Text(key), new IntWritable(sum));
        }

        public LinkedHashMap<Text, IntWritable> sortHashMapByValues(
                HashMap<Text, IntWritable> passedMap) {
            List<Text> mapKeys = new ArrayList<>(passedMap.keySet());
            List<IntWritable> mapValues = new ArrayList<>(passedMap.values());
            Collections.sort(mapValues);
            Collections.sort(mapKeys);

            LinkedHashMap<Text, IntWritable> sortedMap =
                    new LinkedHashMap<>();

            Iterator<IntWritable> valueIt = mapValues.iterator();
            while (valueIt.hasNext()) {
                IntWritable val = valueIt.next();
                Iterator<Text> keyIt = mapKeys.iterator();

                while (keyIt.hasNext()) {
                    Text key = keyIt.next();
                    IntWritable comp1 = passedMap.get(key);
                    IntWritable comp2 = val;

                    if (comp1.equals(comp2)) {
                        keyIt.remove();
                        sortedMap.put(key, val);
                        break;
                    }
                }
            }
            return sortedMap;
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            Map<Text, IntWritable> sortedMap = sortHashMapByValues(countMap);
            Scanner scanner = new Scanner(new File("/usr/local/hadoop/Nword.txt"));
            int Nword = scanner.nextInt();
            int counter = 0;
            int TotalNumber = sortedMap.size();
            for (Text key : sortedMap.keySet()) {
                if (counter++ <= TotalNumber - Nword) {
                    continue;
                }
                context.write(key, sortedMap.get(key));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <deadline> <desired number of maps> <in> [<in>...] <out> ");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "WordMedian");
        job.setJarByClass(WordMedian.class);
        job.setMapperClass(WordMedianMapper.class);
        //job.setCombinerClass(CombinerTopN.class);
        job.setReducerClass(ReducerMedian.class);
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
