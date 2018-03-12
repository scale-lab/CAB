package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import org.apache.hadoop.mapred.Reporter;


/**
 * This is an example Hadoop Map/Reduce application.
 * It produces list of out-links and in-links for each host.
 * Map reads the text input files each line of the format {A1,A2}, and produces <A1,from{}:to{A2}> and <A2,from{A1}:to{}> tuples
 * as Map output.
 * The Reduce output is a union of all such lists with same key.
 * 
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar adjlist
 *            [-m <i>maps</i>] [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i> 
 *
 * @author Faraz Ahmad
 */

@SuppressWarnings("deprecation")
public class AdjList {

  private enum Counter { WORDS, VALUES }

  public static final Log LOG = LogFactory.getLog("org.apache.hadoop.examples.AdjList");
  public static final int LIMIT = 200000;

  public static class MapAdjlist extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, 
        Context context) throws IOException, InterruptedException {

      String line = ((Text)value).toString();
      int index = line.lastIndexOf(",");
      if(index == -1) {
        LOG.info("MAP INPUT IN WRONG FORMAT : " + line);
      }
      String outEdge = line.substring(0,index);
      String inEdge = line.substring(index+1);
      String outList = "from{" + outEdge + "}:to{}";
      String inList = "from{}:to{" + inEdge + "}"; 

      context.write(new Text(outEdge), new Text(inList));
      //reporter.incrCounter(Counter.WORDS, 1);
      context.write(new Text(inEdge),new Text(outList));
      //reporter.incrCounter(Counter.WORDS, 1);
    }
  }

  /**
   * A reducer class that makes union of all lists.
   */
  public static class ReduceAdjlist extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values,
        Context context) throws IOException, InterruptedException {

      List<String> fromList = new ArrayList<String>();
      List<String> toList = new ArrayList<String>();
      String str = new String();
      String fromLine = new String();
      String toLine = new String();
      String vertex = new String();
      Text outValue = new Text();
      int r, strLength, index;

      while (values.hasNext()) {
        str = ((Text) values.next()).toString(); 
        strLength = str.length();
        index = str.indexOf(":");
        if(index == -1) {
          LOG.info("REDUCE INPUT IN WRONG FORMAT : " + str);
          continue;
        }
        if(index > 6) // non-empty fromList
          fromLine = str.substring(5,index-1);
        if(index + 5 < strLength) // non-empty toList
          toLine  = str.substring(index+4, strLength-1);

        if(!fromLine.isEmpty()){
          StringTokenizer itr = new StringTokenizer(fromLine,",");
          while(itr.hasMoreTokens()) {
            vertex = new String(itr.nextToken());
            if(!fromList.contains(vertex) && fromList.size() < LIMIT) //avoid heap overflow
              fromList.add(vertex);
          }
        }
        if(!toLine.isEmpty()){
          StringTokenizer itr = new StringTokenizer(toLine,",");
          while(itr.hasMoreTokens()) {
            vertex = new String(itr.nextToken());
            if(!toList.contains(vertex) && toList.size() < LIMIT) // avoid heap overflow
              toList.add(vertex);
          }	
        }
      }
      Collections.sort(fromList);
      Collections.sort(toList);
      String fromList_str = new String("");
      String toList_str = new String("");
      for (r = 0; r < fromList.size(); r++)
        if(fromList_str.equals(""))
          fromList_str = fromList.get(r);
        else
          fromList_str = fromList_str + "," + fromList.get(r);
      for (r = 0; r < toList.size(); r++)
        if(toList_str.equals(""))
          toList_str = toList.get(r);
        else
          toList_str = toList_str + "," + toList.get(r);

      outValue = new Text("from{" + fromList_str + "}:to{" + toList_str + "}");
      context.write(key,outValue);
    }
  }

  static void printUsage() {
    System.out.println("adjlist [-m <maps>] [-r <reduces>] <input> <output>");
    System.exit(1);
  }

  /**
   * The main driver for word count map/reduce program.
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


        Job job = Job.getInstance(conf, "adjlist");
        job.setJarByClass(AdjList.class);
        job.setMapperClass(MapAdjlist.class);
        job.setReducerClass(ReduceAdjlist.class);
        job.setCombinerClass(ReduceAdjlist.class);
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
