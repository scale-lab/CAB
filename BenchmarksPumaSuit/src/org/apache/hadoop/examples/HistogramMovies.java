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
 * It reads the input movie files and outputs a histogram showing how many movies fall in which
 * category of reviews. 
 * We make 8 reduce tasks for 1-1.5,1.5-2,2-2.5,2.5-3,3-3.5,3.5-4,4-4.5,4.5-5.
 * the reduce task counts all reviews in the same bin.
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar histogram_movies
 *            [-m <i>maps</i>] [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i> 
 *
 * @author Faraz Ahmad
 */
@SuppressWarnings("deprecation")
public class HistogramMovies {

    private enum Counter {

        WORDS, VALUES
    }
    //public static final Log LOG = LogFactory.getLog("org.apache.hadoop.examples.HistogramMovies");

    public static class MapHisMov extends Mapper<Object, Text, FloatWritable, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private final static float division = 0.5f;

        public void map(Object key, Text value,
                Context context) throws IOException, InterruptedException {

            int rating, movieIndex, reviewIndex;
            int totalReviews = 0, sumRatings = 0;
            float avgReview = 0.0f, absReview, fraction, outValue = 0.0f;
            String reviews = new String();
            String line = new String();
            String tok = new String();
            String ratingStr = new String();

            line = ((Text) value).toString();
            movieIndex = line.indexOf(":");
            if (movieIndex > 0) {
                reviews = line.substring(movieIndex + 1);
                StringTokenizer token = new StringTokenizer(reviews, ",");
                while (token.hasMoreTokens()) {
                    tok = token.nextToken();
                    reviewIndex = tok.indexOf("_");
                    ratingStr = tok.substring(reviewIndex + 1);
                    rating = Integer.parseInt(ratingStr);
                    sumRatings += rating;
                    totalReviews++;
                }
                avgReview = (float) sumRatings / (float) totalReviews;
                absReview = (float) Math.floor((double) avgReview);

                fraction = avgReview - absReview;
                int limitInt = Math.round(1.0f / division);

                for (int i = 1; i <= limitInt; i++) {
                    if (fraction < (division * i)) {
                        outValue = absReview + division * i;
                        break;
                    }
                }
                context.write(new FloatWritable(outValue), one);
            }
        }
    }

    public static class ReduceHisMov extends Reducer<FloatWritable, IntWritable, FloatWritable, IntWritable> {
        
        private IntWritable result = new IntWritable();

        public void reduce(FloatWritable key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    static void printUsage() {
        System.out.println("histogram_movies [-m <maps>] [-r <reduces>] <input> <output>");
        System.exit(1);
    }
    /**
     * The main driver for histogram map/reduce program.
     * Invoke this method to submit the map/reduce job.
     * @throws IOException When there is communication problems with the 
     *                     job tracker.
     */

  public  static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 4) {
            System.err.println("Usage: rankedinvertedindex <deadline> <desired number of maps> <in> [<in>...] <out> ");
            System.exit(2);
        }


        Job job = Job.getInstance(conf, "HistogramMoveis"
                + " Deadline");
        job.setJarByClass(HistogramMovies.class);
        job.setMapperClass(MapHisMov.class);
        job.setReducerClass(ReduceHisMov.class);
        job.setCombinerClass(ReduceHisMov.class);
        job.setMapOutputKeyClass(FloatWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(FloatWritable.class);
        job.setOutputValueClass(IntWritable.class);





        job.setDeadline(new Integer(otherArgs[0]));
        job.setDesiredMap(new Integer(otherArgs[1]));
        FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
