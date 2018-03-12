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

import java.io.IOException;



import java.util.HashMap;
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

public class WordMean  {
    
   

  public static class MapMean
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                if (word.toString().length() >= 8 && word.toString().matches("^[a-zA-Z]+$")) {
                    context.write(word, one);
                }

            }
        }
    }

    public static class CombinerMean
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

    public static class ReduceMean
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

        protected void cleanup(Context context) throws IOException, InterruptedException {
            long counter = 0;
            long length = 0;
            for (Text key : countMap.keySet()) {
                int temp1 = key.toString().length();
                int temp2 = Integer.parseInt(countMap.get(key).toString());
                counter += temp2;
                length += temp2 * temp1;

            }

            double mean = (((double) length) / ((double) counter));

            context.write(new Text(Double.toString(mean)), new IntWritable(1));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <deadline> <desired number of maps> <in> [<in>...] <out> ");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Word Mean Deadline");
        job.setJarByClass(WordMean.class);
        job.setMapperClass(MapMean.class);
        job.setCombinerClass(CombinerMean.class);
        job.setReducerClass(ReduceMean.class);
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