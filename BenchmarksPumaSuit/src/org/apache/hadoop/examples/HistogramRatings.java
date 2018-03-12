package org.apache.hadoop.examples;

import java.io.IOException;
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

/**
 * This is an example Hadoop Map/Reduce application.
 * It reads the input movie files and outputs a histogram showing how many reviews fall in which range
 * We make 5 reduce tasks for ratings of 1, 2, 3, 4, and 5.
 * the reduce task counts all reviews in the same bin and emits them.
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar histogram_ratings
 *            [-m <i>maps</i>] [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i> 
 *
 * @author Faraz Ahmad
 */
@SuppressWarnings("deprecation")
public class HistogramRatings {

  private enum Counter { WORDS, VALUES }

  //public static final Log LOG = LogFactory.getLog("org.apache.hadoop.examples.HistogramRatings");

  public static class MapHisRat extends  Mapper<Object, Text, IntWritable, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, 
        Context context) throws IOException, InterruptedException {

      int rating, reviewIndex, movieIndex;
      String reviews = new String();
      String tok = new String();
      String ratingStr = new String();

      String line = ((Text)value).toString();
      movieIndex = line.indexOf(":");
      if (movieIndex > 0) {
        reviews = line.substring(movieIndex + 1);
        StringTokenizer token = new StringTokenizer(reviews, ",");
        while (token.hasMoreTokens()) {
          tok = token.nextToken();
          reviewIndex = tok.indexOf("_");
          ratingStr = tok.substring(reviewIndex + 1);
          rating = Integer.parseInt(ratingStr);
          context.write(new IntWritable(rating), one);
        }
      }
    }
  }

  public static class ReduceHisRat extends  Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    public void reduce( IntWritable key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {

      int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
      context.write(key, new IntWritable(sum));
    }

  }


  static void printUsage() {
    System.out.println("histogram_ratings [-m <maps>] [-r <reduces>] <input> <output>");
    System.exit(1);
  }


  /**
   * The main driver for histogram_ratings map/reduce program.
   * Invoke this method to submit the map/reduce job.
   * @throws IOException When there is communication problems with the 
   *                     job tracker.
   */

  public  static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 4) {
            System.err.println("Usage: HistogramRatings <deadline> <desired number of maps> <in> [<in>...] <out> ");
            System.exit(2);
        }


        Job job = Job.getInstance(conf, "HistogramRatings"
                + " Deadline");
        job.setJarByClass(HistogramRatings.class);
        job.setMapperClass(MapHisRat.class);
        job.setReducerClass(ReduceHisRat.class);
        job.setCombinerClass(ReduceHisRat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);


        job.setDeadline(new Integer(otherArgs[0]));
        job.setDesiredMap(new Integer(otherArgs[1]));
        FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
