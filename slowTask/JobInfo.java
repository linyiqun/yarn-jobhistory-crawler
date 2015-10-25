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
package org.apache.hadoop.mapreduce.v2.hs.webapp.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.ConfEntryInfo;
import org.apache.hadoop.mapreduce.v2.hs.CompletedJob;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRApps.TaskAttemptStateUI;
import org.apache.hadoop.security.authorize.AccessControlList;

@XmlRootElement(name = "job")
@XmlAccessorType(XmlAccessType.FIELD)
public class JobInfo {

  protected long submitTime;
  protected long startTime;
  protected long finishTime;
  protected String id;
  protected String name;
  protected String queue;
  protected String user;
  protected String state;
  protected int mapsTotal;
  protected int mapsCompleted;
  protected int reducesTotal;
  protected int reducesCompleted;
  protected Boolean uberized;
  protected String diagnostics;
  protected Long avgMapTime;
  protected Long avgReduceTime;
  protected Long avgShuffleTime;
  protected Long avgMergeTime;
  protected Long avgMapElapsedTime;
  protected Long avgReduceElapsedTime;
  protected Long avgMapGcTime;
  protected Long avgReduceGcTime;
  protected Long avgMapStartTime;
  protected Long avgReduceStartTime;
  protected Long avgMapReadBytes;
  protected Long avgMapWriteBytes;
  protected Long avgReduceReadBytes;
  protected Long avgReduceWriteBytes;
  protected Integer failedReduceAttempts;
  protected Integer killedReduceAttempts;
  protected Integer successfulReduceAttempts;
  protected Integer failedMapAttempts;
  protected Integer killedMapAttempts;
  protected Integer successfulMapAttempts;
  protected ArrayList<ConfEntryInfo> acls;
  
  //add new metric infos
  protected Long maxMapTime;
  protected Long minMapTime;
  protected Long maxReduceTime;
  protected Long minReduceTime;
  protected Long firstMapStartTime;
  protected Long firstReduceStartTime;
  protected Long lastMapStartTime;
  protected Long lastReduceStartTime;
  
  @XmlTransient
  protected int numMaps;
  @XmlTransient
  protected int numReduces;
  
  public JobInfo() {
  }

  public JobInfo(Job job) {
    this.id = MRApps.toString(job.getID());
    JobReport report = job.getReport();
    
    this.mapsTotal = job.getTotalMaps();
    this.mapsCompleted = job.getCompletedMaps();
    this.reducesTotal = job.getTotalReduces();
    this.reducesCompleted = job.getCompletedReduces();
    this.submitTime = report.getSubmitTime();
    this.startTime = report.getStartTime();
    this.finishTime = report.getFinishTime();
    this.name = job.getName().toString();
    this.queue = job.getQueueName();
    this.user = job.getUserName();
    this.state = job.getState().toString();

    this.acls = new ArrayList<ConfEntryInfo>();
    
    if (job instanceof CompletedJob) {
      avgMapTime = 0l;
      avgReduceTime = 0l;
      avgShuffleTime = 0l;
      avgMergeTime = 0l;
      avgMapGcTime = 0L;
      avgMapElapsedTime = 0L;
      avgReduceElapsedTime = 0L;
      avgReduceGcTime = 0L;
      avgMapStartTime = 0L;
      avgReduceStartTime = 0L;
      avgMapReadBytes = 0L;
      avgMapWriteBytes = 0L;
      avgReduceReadBytes = 0L;
      avgReduceWriteBytes = 0L;
      failedReduceAttempts = 0;
      killedReduceAttempts = 0;
      successfulReduceAttempts = 0;
      failedMapAttempts = 0;
      killedMapAttempts = 0;
      successfulMapAttempts = 0;
      minMapTime = Long.MAX_VALUE;
      maxMapTime = Long.MIN_VALUE;
      minReduceTime = Long.MAX_VALUE;
      maxReduceTime = Long.MIN_VALUE;
      firstMapStartTime = Long.MAX_VALUE;
      lastMapStartTime = Long.MIN_VALUE;
      firstReduceStartTime = Long.MAX_VALUE;
      lastReduceStartTime = Long.MIN_VALUE;
      countTasksAndAttempts(job);
      
      this.uberized = job.isUber();
      this.diagnostics = "";
      List<String> diagnostics = job.getDiagnostics();
      if (diagnostics != null && !diagnostics.isEmpty()) {
        StringBuffer b = new StringBuffer();
        for (String diag : diagnostics) {
          b.append(diag);
        }
        this.diagnostics = b.toString();
      }


      Map<JobACL, AccessControlList> allacls = job.getJobACLs();
      if (allacls != null) {
        for (Map.Entry<JobACL, AccessControlList> entry : allacls.entrySet()) {
          this.acls.add(new ConfEntryInfo(entry.getKey().getAclName(), entry
              .getValue().getAclString()));
        }
      }
    }
  }

  public long getNumMaps() {
    return numMaps;
  }

  public long getNumReduces() {
    return numReduces;
  }

  public Long getAvgMapTime() {
    return avgMapTime;
  }

  public Long getAvgReduceTime() {
    return avgReduceTime;
  }

  public Long getAvgShuffleTime() {
    return avgShuffleTime;
  }

  public Long getAvgMergeTime() {
    return avgMergeTime;
  }

  public Integer getFailedReduceAttempts() {
    return failedReduceAttempts;
  }

  public Integer getKilledReduceAttempts() {
    return killedReduceAttempts;
  }

  public Integer getSuccessfulReduceAttempts() {
    return successfulReduceAttempts;
  }

  public Integer getFailedMapAttempts() {
    return failedMapAttempts;
  }

  public Integer getKilledMapAttempts() {
    return killedMapAttempts;
  }

  public Integer getSuccessfulMapAttempts() {
    return successfulMapAttempts;
  }

  public ArrayList<ConfEntryInfo> getAcls() {
    return acls;
  }

  public int getReducesCompleted() {
    return this.reducesCompleted;
  }

  public int getReducesTotal() {
    return this.reducesTotal;
  }

  public int getMapsCompleted() {
    return this.mapsCompleted;
  }

  public int getMapsTotal() {
    return this.mapsTotal;
  }

  public String getState() {
    return this.state;
  }

  public String getUserName() {
    return this.user;
  }

  public String getName() {
    return this.name;
  }

  public String getQueueName() {
    return this.queue;
  }

  public String getId() {
    return this.id;
  }

  public long getSubmitTime() {
      return this.submitTime;
  }

  public long getStartTime() {
    return this.startTime;
  }

  public long getFinishTime() {
    return this.finishTime;
  }

  public Boolean isUber() {
    return this.uberized;
  }

  public String getDiagnostics() {
    return this.diagnostics;
  }
  
  public long getMaxMapTime() {
    return this.maxMapTime;
  }

  public long getMinMapTime() {
    return this.minMapTime;
  }

  public long getMaxReduceTime() {
    return this.maxReduceTime;
  }

  public long getMinReduceTime() {
    return this.minReduceTime;
  }
  
  public long getFirstMapStartTime() {
    return this.firstMapStartTime;
  }

  public long getFirstReduceStartTime() {
    return this.firstMapStartTime;
  }

  public long getLastMapStartTime() {
    return this.lastMapStartTime;
  }

  public long getLastReduceStartTime() {
    return this.lastMapStartTime;
  }
  
  public long getAvgMapStartTime() {
    return (long) this.avgMapStartTime;
  }

  public long getAvgReduceStartTime() {
    return (long) this.avgReduceStartTime;
  }
  
  public long getAvgMapGcTime() {
    return this.avgMapGcTime;
  }

  public long getAvgReduceGcTime() {
    return this.avgReduceGcTime;
  }
  
  public long getAvgMapElapsedTime() {
    return this.avgMapElapsedTime;
  }

  public long getAvgReduceElapsedTime() {
    return this.avgReduceElapsedTime;
  }
  
  public long getAvgMapReadBytes() {
    return this.avgMapReadBytes;
  }

  public long getAvgMapWriteBytes() {
    return this.avgMapWriteBytes;
  }

  public long getAvgReduceReadBytes() {
    return this.avgReduceReadBytes;
  }

  public long getAvgReduceWriteBytes() {
    return this.avgReduceWriteBytes;
  }
  
  /**
   * Go through a job and update the member variables with counts for
   * information to output in the page.
   *
   * @param job
   *          the job to get counts for.
   */
  private void countTasksAndAttempts(Job job) {
    numReduces = 0;
    numMaps = 0;
    final Map<TaskId, Task> tasks = job.getTasks();
    if (tasks == null) {
      return;
    }
    for (Task task : tasks.values()) {
      // Attempts counts
      Map<TaskAttemptId, TaskAttempt> attempts = task.getAttempts();
      int successful, failed, killed;
      for (TaskAttempt attempt : attempts.values()) {

        successful = 0;
        failed = 0;
        killed = 0;
        if (TaskAttemptStateUI.NEW.correspondsTo(attempt.getState())) {
          // Do Nothing
        } else if (TaskAttemptStateUI.RUNNING.correspondsTo(attempt.getState())) {
          // Do Nothing
        } else if (TaskAttemptStateUI.SUCCESSFUL.correspondsTo(attempt
            .getState())) {
          ++successful;
        } else if (TaskAttemptStateUI.FAILED.correspondsTo(attempt.getState())) {
          ++failed;
        } else if (TaskAttemptStateUI.KILLED.correspondsTo(attempt.getState())) {
          ++killed;
        }
        
        long tmp;
        long gcTime;
        long readBytes;
        long writeBytes;
        Counters counters;
        
        gcTime = 0;
        readBytes = 0;
        writeBytes = 0;
        counters = null;
        if(task.getReport() != null){
          counters = task.getReport().getCounters();
        }
        
        if(counters != null && counters.getCounter(TaskCounter.GC_TIME_MILLIS) != null){
          gcTime = counters.getCounter(TaskCounter.GC_TIME_MILLIS).getValue();
        }
        
        if(counters != null && counters.getCounter(FileSystemCounter.BYTES_READ) != null){
          readBytes = counters.getCounter(FileSystemCounter.BYTES_READ).getValue();
        }
        
        if(counters != null && counters.getCounter(FileSystemCounter.BYTES_WRITTEN) != null){
          writeBytes = counters.getCounter(FileSystemCounter.BYTES_WRITTEN).getValue();
        }
        
        switch (task.getType()) {
        case MAP:
          successfulMapAttempts += successful;
          failedMapAttempts += failed;
          killedMapAttempts += killed;
          if (attempt.getState() == TaskAttemptState.SUCCEEDED) {
            numMaps++;
            avgMapTime += (attempt.getFinishTime() - attempt.getLaunchTime());
            avgMapGcTime += gcTime;
            avgMapStartTime += attempt.getLaunchTime();
            avgMapReadBytes += readBytes;
            avgMapWriteBytes += writeBytes;
            
            tmp = attempt.getFinishTime() - attempt.getLaunchTime();
            if(tmp > maxMapTime){
            	maxMapTime = tmp;
            }
            
            if (tmp < minMapTime){
            	minMapTime = tmp;
            }
            
            if(attempt.getLaunchTime() < firstMapStartTime){
              firstMapStartTime = attempt.getLaunchTime();
            }
            
            if(attempt.getLaunchTime() > lastMapStartTime){
              lastMapStartTime = attempt.getLaunchTime();
            }
            
            
          }
          break;
        case REDUCE:
          successfulReduceAttempts += successful;
          failedReduceAttempts += failed;
          killedReduceAttempts += killed;
          if (attempt.getState() == TaskAttemptState.SUCCEEDED) {
            numReduces++;
            avgShuffleTime += (attempt.getShuffleFinishTime() - attempt
                .getLaunchTime());
            avgMergeTime += attempt.getSortFinishTime()
                - attempt.getShuffleFinishTime();
            avgReduceTime += (attempt.getFinishTime() - attempt
                .getSortFinishTime());
            avgReduceStartTime += attempt.getLaunchTime();
            avgReduceGcTime += gcTime;
            avgReduceElapsedTime += attempt.getFinishTime() - attempt
                .getLaunchTime(); 
            avgReduceReadBytes += readBytes;
            avgReduceWriteBytes += writeBytes;
            
            tmp = attempt.getFinishTime() - attempt.getLaunchTime();
            if(tmp > maxReduceTime){
              maxReduceTime = tmp;
            }
            
            if (tmp < minReduceTime){
              minReduceTime = tmp;
            }
            
            if(attempt.getLaunchTime() < firstReduceStartTime){
              firstReduceStartTime = attempt.getLaunchTime();
            }
            
            if(attempt.getLaunchTime() > lastReduceStartTime){
              lastReduceStartTime = attempt.getLaunchTime();
            }
          }
          break;
        }
      }
    }

    if (numMaps > 0) {
      avgMapTime = avgMapTime / numMaps;
      avgMapGcTime = avgMapGcTime / numMaps;
      avgMapStartTime = avgMapStartTime / numMaps;
      avgMapReadBytes = avgMapReadBytes / numMaps;
      avgMapWriteBytes = avgMapWriteBytes / numMaps;
      
      avgMapElapsedTime = avgMapTime;
    }

    if (numReduces > 0) {
      avgReduceTime = avgReduceTime / numReduces;
      avgShuffleTime = avgShuffleTime / numReduces;
      avgMergeTime = avgMergeTime / numReduces;
      avgReduceElapsedTime = avgReduceElapsedTime / numReduces;
      avgReduceGcTime = avgReduceGcTime / numReduces;
      avgReduceStartTime = avgReduceStartTime / numReduces;
      avgReduceReadBytes = avgReduceReadBytes / numReduces;
      avgReduceWriteBytes = avgReduceWriteBytes / numReduces;
    }
  }
  
  public Map<TaskId, Task> getElapsedSlowTasks(Job job, double ratio) {
    long tmpElapsedTime;
    double mapElapsedThresholdTime;
    double reduceElapsedThresholdTime;
    Map<TaskId, Task> slowTasks;
    
    slowTasks = new HashMap<TaskId, Task>();

    mapElapsedThresholdTime = avgMapElapsedTime * (1 + ratio);
    reduceElapsedThresholdTime = avgReduceElapsedTime * (1 + ratio);

    final Map<TaskId, Task> tasks = job.getTasks();
    if (tasks == null) {
      return slowTasks;
    }

    for (Task task : tasks.values()) {
      tmpElapsedTime =
          task.getReport().getFinishTime() - task.getReport().getStartTime();

      if (task.getType() == TaskType.MAP
          && tmpElapsedTime >= mapElapsedThresholdTime) {
        slowTasks.put(task.getID(), task);
      } else if (task.getType() == TaskType.REDUCE
          && tmpElapsedTime >= reduceElapsedThresholdTime) {
        slowTasks.put(task.getID(), task);
      }
    }

    return slowTasks;
  }
  
  public int[] containElapsedSlowTasks(Job job, double ratio){
    int[] isContained;
    long tmpElapsedTime;
    double mapElapsedThresholdTime;
    double reduceElapsedThresholdTime;
    
    mapElapsedThresholdTime = avgMapElapsedTime * (1 + ratio);
    reduceElapsedThresholdTime = avgReduceElapsedTime * (1 + ratio);

    final Map<TaskId, Task> tasks = job.getTasks();
    
    isContained = new int[2];
    isContained[0] = 0;
    isContained[1] = 0;
    if (tasks == null) {
      return isContained;
    }

    for (Task task : tasks.values()) {
      tmpElapsedTime =
          task.getReport().getFinishTime() - task.getReport().getStartTime();

      if (task.getType() == TaskType.MAP
          && tmpElapsedTime >= mapElapsedThresholdTime) {
        isContained[0] = 1;
      } else if (task.getType() == TaskType.REDUCE
          && tmpElapsedTime >= reduceElapsedThresholdTime) {
        isContained[1] = 1;
      }
      
      if(isContained[0] == 1 && isContained[1] == 1){
        break;
      }
    }

    return isContained;
  }

  public Map<TaskId, Task> getGcSlowTasks(Job job, double ratio) {
    long tmpGcTime;
    double mapGcThresholdTime;
    double reduceGcThresholdTime;

    Counters counters;
    Map<TaskId, Task> slowTasks;
    
    slowTasks = new HashMap<TaskId, Task>();

    mapGcThresholdTime = avgMapGcTime * (1 + ratio);
    reduceGcThresholdTime = avgReduceGcTime * (1 + ratio);

    final Map<TaskId, Task> tasks = job.getTasks();
    if (tasks == null) {
      return slowTasks;
    }

    for (Task task : tasks.values()) {
      counters = task.getReport().getCounters();

      tmpGcTime = 0;
      if (counters != null && counters.getCounter(TaskCounter.GC_TIME_MILLIS) != null) {
        tmpGcTime = counters.getCounter(TaskCounter.GC_TIME_MILLIS).getValue();
      }

      if (task.getType() == TaskType.MAP && tmpGcTime >= mapGcThresholdTime) {
        slowTasks.put(task.getID(), task);
      } else if (task.getType() == TaskType.REDUCE
          && tmpGcTime >= reduceGcThresholdTime) {
        slowTasks.put(task.getID(), task);
      }
    }

    return slowTasks;
  }
  
  public int[] containGcSlowTasks(Job job, double ratio){
    int[] isContained;
    long tmpGcTime;
    double mapGcThresholdTime;
    double reduceGcThresholdTime;

    Counters counters;
    
    mapGcThresholdTime = avgMapGcTime * (1 + ratio);
    reduceGcThresholdTime = avgReduceGcTime * (1 + ratio);

    final Map<TaskId, Task> tasks = job.getTasks();
    isContained = new int[2];
    isContained[0] = 0;
    isContained[1] = 0;
    if (tasks == null) {
      return isContained;
    }

    for (Task task : tasks.values()) {
      counters = task.getReport().getCounters();

      tmpGcTime = 0;
      if (counters != null && counters.getCounter(TaskCounter.GC_TIME_MILLIS) != null) {
        tmpGcTime = counters.getCounter(TaskCounter.GC_TIME_MILLIS).getValue();
      }

      if (task.getType() == TaskType.MAP && tmpGcTime >= mapGcThresholdTime) {
        isContained[0] = 1;
      } else if (task.getType() == TaskType.REDUCE
          && tmpGcTime >= reduceGcThresholdTime) {
        isContained[1] = 1;
      }
      
      if(isContained[0] == 1 && isContained[1] == 1){
        break;
      }
    }

    return isContained;
  }
  
  public Map<TaskId, Task> getSlowStartTasks(Job job, double ratio) {
    long tmpStartElapsedTime;
    double mapSlowStartThresholdTime;
    double reduceSlowStartThresholdTime;

    Map<TaskId, Task> slowTasks;
    slowTasks = new HashMap<TaskId, Task>();

    mapSlowStartThresholdTime =
        (avgMapStartTime - firstMapStartTime) * (1 + ratio);
    reduceSlowStartThresholdTime =
        (avgReduceStartTime - firstReduceStartTime) * (1 + ratio);

    final Map<TaskId, Task> tasks = job.getTasks();
    if (tasks == null) {
      return slowTasks;
    }

    for (Task task : tasks.values()) {
      if (task.getType() == TaskType.MAP && mapSlowStartThresholdTime > 0) {
        tmpStartElapsedTime =
            task.getReport().getStartTime() - firstMapStartTime;

        if (tmpStartElapsedTime >= mapSlowStartThresholdTime) {
          slowTasks.put(task.getID(), task);
        }
      } else if (task.getType() == TaskType.REDUCE && reduceSlowStartThresholdTime > 0) {
        tmpStartElapsedTime =
            task.getReport().getStartTime() - firstReduceStartTime;

        if (tmpStartElapsedTime >= reduceSlowStartThresholdTime) {
          slowTasks.put(task.getID(), task);
        }
      }
    }

    return slowTasks;
  }
  
  public int[] containSlowStartTasks(Job job, double ratio){
    int[] isContained;
    long tmpStartElapsedTime;
    double mapSlowStartThresholdTime;
    double reduceSlowStartThresholdTime;

    mapSlowStartThresholdTime =
        (avgMapStartTime - firstMapStartTime) * (1 + ratio);
    reduceSlowStartThresholdTime =
        (avgReduceStartTime - firstReduceStartTime) * (1 + ratio);

    final Map<TaskId, Task> tasks = job.getTasks();
    isContained = new int[2];
    isContained[0] = 0;
    isContained[1] = 0;
    if (tasks == null) {
      return isContained;
    }

    for (Task task : tasks.values()) {
      if (task.getType() == TaskType.MAP && mapSlowStartThresholdTime > 0) {
        tmpStartElapsedTime =
            task.getReport().getStartTime() - firstMapStartTime;

        if (tmpStartElapsedTime >= mapSlowStartThresholdTime) {
          isContained[0] = 1;
        }
      } else if (task.getType() == TaskType.REDUCE && reduceSlowStartThresholdTime > 0) {
        tmpStartElapsedTime =
            task.getReport().getStartTime() - firstReduceStartTime;

        if (tmpStartElapsedTime >= reduceSlowStartThresholdTime) {
          isContained[1] = 1;
        }
      }
      
      if(isContained[0] == 1 && isContained[1] == 1){
        break;
      }
    }

    return isContained;
  }
  
  public Map<TaskId, Task> getReadDataLeanTasks(Job job, double ratio) {
    long tmpReadBytesNum;
    double readBytesThresholdNum;

    Counters counters;
    Map<TaskId, Task> slowTasks;
    
    slowTasks = new HashMap<TaskId, Task>();

    readBytesThresholdNum = avgMapReadBytes * (1 + ratio);

    final Map<TaskId, Task> tasks = job.getTasks();
    if (tasks == null) {
      return slowTasks;
    }

    for (Task task : tasks.values()) {
      counters = task.getReport().getCounters();

      tmpReadBytesNum = 0;
      if (counters != null && counters.getCounter(FileSystemCounter.BYTES_READ) != null) {
        tmpReadBytesNum = counters.getCounter(FileSystemCounter.BYTES_READ).getValue();
      }

      if (task.getType() == TaskType.MAP && tmpReadBytesNum >= readBytesThresholdNum) {
        slowTasks.put(task.getID(), task);
      }
    }

    return slowTasks;
  }
  
  public Map<TaskId, Task> getWriteDataLeanTasks(Job job, double ratio) {
    long tmpWriteBytesNum;
    double writeBytesThresholdNum;

    Counters counters;
    Map<TaskId, Task> slowTasks;
    
    slowTasks = new HashMap<TaskId, Task>();

    writeBytesThresholdNum = avgReduceWriteBytes * (1 + ratio);

    final Map<TaskId, Task> tasks = job.getTasks();
    if (tasks == null) {
      return slowTasks;
    }

    for (Task task : tasks.values()) {
      counters = task.getReport().getCounters();

      tmpWriteBytesNum = 0;
      if (counters != null && counters.getCounter(FileSystemCounter.BYTES_WRITTEN) != null) {
        tmpWriteBytesNum = counters.getCounter(FileSystemCounter.BYTES_WRITTEN).getValue();
      }

      if (task.getType() == TaskType.REDUCE && tmpWriteBytesNum >= writeBytesThresholdNum) {
        slowTasks.put(task.getID(), task);
      }
    }

    return slowTasks;
  }

}
