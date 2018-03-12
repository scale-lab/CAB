/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

/**
 *
 * @author Morteza
 */

// This is a new class to handle the jobs 
//some of the variables are not being used in current version. But later we might need them.
public class JobInformation {
    public int minimumRequiredTasks;
    public int executedTasks;
    public int deadline;
    public int elapsedTime;
    public String jobID;
    public String nameofQueue;
    public int memoryUsage; //Queue Usage Memory
    public int vcoresUsage; //Queue USage Core
    public int MaxMemory; //Queue Max Memory
    public int MaxCore;   //Queue Max Core
    public int containerUsage;
    public int MaxContainer;
    public int requiredContainer;
    public int extraContainer;
    public int estimatedFinishTime;
    public int neededNumberofContainers;
    public boolean needsResource;
    public boolean hasExtraResource;
    public int [] windowTask = new int[10000];
    public int windowCounter;
    public boolean hasMetMinTask;
    public int tempShare;  //to temporarly save share. it is not sth permanent.
   
    
    public JobInformation()
    {
        containerUsage = 0;
        MaxContainer = 1;  // change to see what happens
        extraContainer = 0;
        requiredContainer = 0;
        needsResource = false;
        hasExtraResource = false;
        minimumRequiredTasks = 0;
        executedTasks = 0;
        deadline = 0;
        elapsedTime = 0;
        windowCounter = 0;
        jobID = "noID";
        nameofQueue = "noQueue";
        estimatedFinishTime = 0;
        neededNumberofContainers = 2;
        hasMetMinTask = false;
        tempShare = 0;
        
    }
       
}
