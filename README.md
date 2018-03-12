In this document, we describe the modules of our CAB approach that we have implemented in Hadoop 2.7.3.

Prior to use the CAB, you should create below folders (alternatively, you should change the address of files that CAB creates to your own desired ones). CAB uses them for communication between its modules:

* /usr/local/hadoop/ApproxCapping
* /usr/local/hadoop/JobFinishTime
* /usr/local/hadoop/JobsLog

Now, we describe each module of CAB:
# 1. CAB_ApproximateResult
After compiling the project, a jar in “dist” folder named hadoop-mapreduce-client-core-2.7.3.jar is created. You should replace this jar file with a jar file with the same name in below address:

/usr/local/hadoop/share/hadoop/mapreduce/

The CAB_ApproximateResult is responsible for calculating the partial result obtained by executing a subset of tasks instead of all of them. By default, when some tasks of job are not completed, the Hadoop would throw some error messages and won’t calculate the result at all. But we have modified this module to let it calculate approximate result.  To implement it, we have modified the below java file:

org.apache.hadoop.mapreduce.task.reduce. ShuffleSchedulerImpl.java

Only a few lines of this file are changed which are designated by //CAB comment in front of them. In ShuffleSchedulerImpl.java, we read the number of tasks that are killed from a file named job_id_KilledCounter.txt and adjust some parameters based on that to make sure that Hadoop returns the partial result. By default, Hadoop expects all the tasks to be finished, and hence, tries to read their output. But, we modify the number of tasks that Hadoop should read their output, based on the number of killed tasks we obtain from job_id_KilledCounter.txt

Another module of CAB called CAB_Sampler_ProgressRateEstimation_ApproximationController is responsible for creating the job_id_KilledCounter.txt file and write number of killed tasks in it. We discuss this module in the following.

# 2. CAB_Sampler_ProgressRateEstimation_ApproximationController
After compiling the project, a jar in “dist” folder named hadoop-mapreduce-client-app-2.7.3.jar is created. You should replace this jar file with a jar file with the same name in below address:

/usr/local/hadoop/share/hadoop/mapreduce/

This module has several responsibilities:
First, it does the sampling to estimate the progress rate of jobs.  After that, it estimates the finish time of job. To implement these functionalities, we have added some lines to below java file:

org.apache.hadoop.mapreduce.v2.app.job.impl.JobImp.java

Lines 884 to 917 do the sampling, progress rate calculation, and job finish time estimation. After that, it writes those to a file called job_id_progress.txt . The resource allocator module of CAB uses this information. We will later discuss about this module.
Another responsibility of this module is to monitor the execution time and executed tasks of job, and terminates the job before it completes its tasks, if necessary. It happens in lines 958 to 1005 of JobImp.java file.

# 3. CAB_ResourceAllocator
After compiling the project, a jar in “dist” folder named hadoop-yarn-server-resourcemanager-2.7.3.jar is created. You should replace this jar file with a jar file with the same name in below address:

/usr/local/hadoop/share/hadoop/yarn/

First of all, we have added JobInformation.java file to below path. It is responsible for storing the information about each running job

org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair

But, the most functionality of this module is implemented in the file FairScheduler.java in the same above path. In this we have implemented several functions. The most important one is the decisionPhase() function which is responsible for allocating resources to queues (or jobs). The decisionPhase() function implements the CAB_allocator algorithm.

# 4. Benchmarks
We have modified four benchmarks (WordCount, WordMean, TopN and Pi) in BenchmarksHadoop2Examples project. These applications can be submitted by indicating their deadline and minimum required number of tasks.

In addition, BenchmarksPumaSuit project contains four applications from Puma suit, Histogram_movies, Histogram_Ratings, InvertedIndex, and Classificaiton. These applications are also modified, so their deadline and minimum required tasks can be specified when submitting them.

# 5. Start Using the Hadoop Cluster
After copying the aforementioned files to specified paths and creating the necessary directories, Hadoop is ready to run the applications. The first step before launching jobs is to restart the Hadoop cluster. To restart the cluster, we need to run to bash files : stop-all.sh & start-all.sh

Note: usually, you can run this bash files without needing to go to their directory. However, if you encounter any problems such as not knowing the bash files, then you should call them using the complete path: 

/usr/local/hadoop/sbin/stop-all.sh
/usr/local/hadoop/sbin/start-all.sh

After the cluster is started successfully, we can monitor the status of HDFS and also the jobs submitted to the cluster using below interfaces (assuming the IP of server is 10.9.84.32):

10.9.84.32:50070   -> HDFS

10.9.84.32:8088     -> Jobs

The next step is to copy the data from local storage to HDFS. We can use below command:

```
hadoop fs -Ddfs.blocksize=16m -copyFromLocal     /home/scale/kmean/    /user/hduser/kmeanInput
```

In the above command, 16m indicates that the size of data blocks is 16 MB.

The address of files in local storage that we are going to copy is /home/scale/kmean/

The address of destination in HDFS is /user/hduser/kmeanInput

Now that we have copied the files, we are ready to submit a job. For example, we want to run the wordcount example of Hadoop on files that are stored in HDFS path /user/hduser/wordInput and save the output in HDFS path /user/hduser/output/wordcount_out  .   The command would be:

```
hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/ hadoop-mapreduce-examples-2.7.3.jar wordcount /user/hduser/wordInput /user/hduser/output/wordcount_out
```

Note: if it did not recognize the hadoop command, then we will use the complete below path:

```
/usr/local/hadoop/bin/hadoop
```
# Contacts
If you use any of our ideas please cite our paper:

S. M. Nabavinejad, X. Zhan, R. Azimi, M Goudarzi, and S. Reda, "QoR-Aware Power Capping for Approximate Big Data Processing," to appear in IEEE Design, Automation Test in Europe, 2018.

Here is our contact information for any further assistant:  

sm.nabavi.nejad at gmail.com  or sherif_reda at brown dot edu

