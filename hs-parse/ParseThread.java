package org.apache.hadoop.mapreduce.v2.hs.tool;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;

public class ParseThread extends Thread{
  private int elapsedTime;
  private double slowRatio;
  
  private HSTool tool;
  private LinkedList<HistoryFileInfo> historyFileInfos;

  public ParseThread(HSTool tool, LinkedList<HistoryFileInfo> historyFileInfos) {
    elapsedTime = tool.elapsedTime;
    slowRatio = tool.slowRatio;

    this.tool = tool;
    this.historyFileInfos = historyFileInfos;
  }

  @Override
  public void run() {
    // TODO Auto-generated method stub
    HistoryFileInfo hfi;

    while (historyFileInfos != null && !historyFileInfos.isEmpty()) {
      hfi = tool.getOneHistoryFileInfo();
      parseCompleteJob(hfi, true);
    }

    super.run();
  }
  
  private void parseCompleteJob(HistoryFileInfo hfi, boolean loadTask) {
    Job job;
    Task task;
    Path confFilePath;
    Configuration conf;
    Map<TaskId, Task> taskInfos;
    
    int isSlowTypeTask;
    String jobName;
    String jobId;
    String user;
    String taId;
    String[] jobValues;
    String[] taskValues;
    String[] taskAttemptValues;
    long jobReadBytes;
    long jobWriteBytes;
    long[] firstTaskStartTime;
    long[][] avgTaskValues;
    double[][] slowTaskRatio;

    job = null;
    conf = null;
    jobReadBytes = 0;
    jobWriteBytes = 0;
    isSlowTypeTask = 0;
    jobId = "";
    jobName = "";
    user = "";
    try {
      job = hfi.loadJob(loadTask);
      confFilePath = hfi.getConfFile();
      conf = loadConfFile(confFilePath);
      
      jobName = job.getName();
      jobId = job.getID().toString();
      user = job.getUserName();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    if (job.getReport() != null
        && ((job.getReport().getFinishTime() - job.getReport().getStartTime()) / 1000) <= elapsedTime) {
      return;
    }

    taskInfos = job.getTasks();
    firstTaskStartTime = new long[2];
    avgTaskValues = calTaskAvgValues(taskInfos, firstTaskStartTime);
    System.out.println("job task total num is " + taskInfos.size());
    
    slowTaskRatio = new double[BaseValues.TASK_AVG_LEN][2];
    
    for (Map.Entry<TaskId, Task> entry : taskInfos.entrySet()) {
      task = entry.getValue();
      isSlowTypeTask = 0;
      
      taskValues = parseTaskInfos(jobId, jobName, user, task, avgTaskValues, firstTaskStartTime);
      
      Map<TaskAttemptId, TaskAttempt> attempts = task.getAttempts();
      for (TaskAttempt attempt : attempts.values()) {
        taskAttemptValues = parseTaskAttemptInfos(jobId, jobName, user, attempt, task.getType(), avgTaskValues);
        taId = attempt.getID().toString();
        
        if(taskAttemptValues != null){
          tool.addDataToTaskAttemptMap(taId, taskAttemptValues);
        }
      } 
      
      if(task.getType() == TaskType.MAP){
        if(taskValues[BaseValues.DB_COLUMN_JOB_TASK_IS_SLOW_RUNNING].equals("1")){
          slowTaskRatio[BaseValues.TASK_AVG_ELAPSED_TIME][0]++;
          isSlowTypeTask = 1;
        }
        
        if(taskValues[BaseValues.DB_COLUMN_JOB_TASK_IS_SLOW_GC].equals("1")){
          slowTaskRatio[BaseValues.TASK_AVG_GC_TIME][0]++;
        }
        
        if(taskValues[BaseValues.DB_COLUMN_JOB_TASK_IS_SLOW_START].equals("1")){
          slowTaskRatio[BaseValues.TASK_AVG_START_TIME][0]++;
        }
        
        if(taskValues[BaseValues.DB_COLUMN_JOB_TASK_IS_READ_DATA_SKEW].equals("1")){
          slowTaskRatio[BaseValues.TASK_AVG_READ_BYTES][0]++;
        }
        
        if(taskValues[BaseValues.DB_COLUMN_JOB_TASK_IS_WRITE_DATA_SKEW].equals("1")){
          slowTaskRatio[BaseValues.TASK_AVG_WRITE_BYTES][0]++;
        }
      }else if(task.getType() == TaskType.REDUCE){
        if(taskValues[BaseValues.DB_COLUMN_JOB_TASK_IS_SLOW_RUNNING].equals("1")){
          slowTaskRatio[BaseValues.TASK_AVG_ELAPSED_TIME][1]++;
          isSlowTypeTask = 1;
        }
        
        if(taskValues[BaseValues.DB_COLUMN_JOB_TASK_IS_SLOW_GC].equals("1")){
          slowTaskRatio[BaseValues.TASK_AVG_GC_TIME][1]++;
        }
        
        if(taskValues[BaseValues.DB_COLUMN_JOB_TASK_IS_SLOW_START].equals("1")){
          slowTaskRatio[BaseValues.TASK_AVG_START_TIME][1]++;
        }
        
        if(taskValues[BaseValues.DB_COLUMN_JOB_TASK_IS_READ_DATA_SKEW].equals("1")){
          slowTaskRatio[BaseValues.TASK_AVG_READ_BYTES][1]++;
        }
        
        if(taskValues[BaseValues.DB_COLUMN_JOB_TASK_IS_WRITE_DATA_SKEW].equals("1")){
          slowTaskRatio[BaseValues.TASK_AVG_WRITE_BYTES][1]++;
        }
      }
      
      jobReadBytes += Long.parseLong(taskValues[BaseValues.DB_COLUMN_JOB_TASK_READ_BYTES]);
      jobWriteBytes += Long.parseLong(taskValues[BaseValues.DB_COLUMN_JOB_TASK_WRITE_BYTES]);
      if (isSlowTypeTask == 1) {
        tool.addDataToTaskDataMap(task.getID().toString(), taskValues);
      }
    }
    
    jobValues = parseJobInfo(job, slowTaskRatio, jobReadBytes, jobWriteBytes, conf);
    tool.addJobCounter();
    tool.addDataToJobInfoMap(jobId, jobValues);
  }
  
  private Configuration loadConfFile(Path confFile) throws IOException {
    FileContext fc = FileContext.getFileContext(confFile.toUri(), new Configuration());
    Configuration jobConf = new Configuration(false);
    jobConf.addResource(fc.open(confFile), confFile.toString());
    
    return jobConf;
  }
  
  private String[] parseJobInfo(Job job, double[][] slowTaskRatios,
      long readBytes, long writeBytes, Configuration conf) {
    String[] values;
    String hiveSql;
    int taskMapNums;
    int taskReduceNums;
    
    taskMapNums = job.getCompletedMaps();
    taskReduceNums = job.getCompletedReduces();
    
    values = new String[BaseValues.DB_COLUMN_HIVE_SQL_LEN];
    values[BaseValues.DB_COLUMN_HIVE_SQL_JOBID] = job.getID().toString();
    values[BaseValues.DB_COLUMN_HIVE_SQL_JOBNAME] = job.getName();
    values[BaseValues.DB_COLUMN_HIVE_SQL_USERNAME] = job.getUserName();
    values[BaseValues.DB_COLUMN_HIVE_SQL_START_TIME] = String.valueOf(job.getReport().getStartTime());
    values[BaseValues.DB_COLUMN_HIVE_SQL_FINISH_TIME] = String.valueOf(job.getReport().getFinishTime());
    values[BaseValues.DB_COLUMN_HIVE_SQL_MAP_TASK_NUM] = String.valueOf(job.getCompletedMaps());
    values[BaseValues.DB_COLUMN_HIVE_SQL_REDUCE_TASK_NUM] = String.valueOf(job.getCompletedReduces());
    values[BaseValues.DB_COLUMN_HIVE_SQL_RUNNING_TIME] = String.valueOf(job.getReport().getFinishTime() - job.getReport().getStartTime());
    values[BaseValues.DB_COLUMN_HIVE_SQL_QUEUE_NAME] = job.getQueueName();
    values[BaseValues.DB_COLUMN_HIVE_SQL_STATUS] = job.getState().toString();
    
    values[BaseValues.DB_COLUMN_HIVE_SQL_MAP_SLOW_ELAPSED_RATIO] =
        String.valueOf(slowTaskRatios[BaseValues.TASK_AVG_ELAPSED_TIME][0]/taskMapNums);
    values[BaseValues.DB_COLUMN_HIVE_SQL_MAP_SLOW_GC_RATIO] =
        String.valueOf(slowTaskRatios[BaseValues.TASK_AVG_GC_TIME][0]/taskMapNums);
    values[BaseValues.DB_COLUMN_HIVE_SQL_MAP_SLOW_START_RATIO] =
        String.valueOf(slowTaskRatios[BaseValues.TASK_AVG_START_TIME][0]/taskMapNums);
    values[BaseValues.DB_COLUMN_HIVE_SQL_MAP_READ_DATA_SKEW_RATIO] =
        String.valueOf(slowTaskRatios[BaseValues.TASK_AVG_READ_BYTES][0]/taskMapNums);
    values[BaseValues.DB_COLUMN_HIVE_SQL_MAP_WRITE_DATA_SKEW_RATIO] =
        String.valueOf(slowTaskRatios[BaseValues.TASK_AVG_WRITE_BYTES][0]/taskMapNums);
    
    values[BaseValues.DB_COLUMN_HIVE_SQL_REDUCE_SLOW_ELAPSED_RATIO] =
        String.valueOf(slowTaskRatios[BaseValues.TASK_AVG_ELAPSED_TIME][1]/taskReduceNums);
    values[BaseValues.DB_COLUMN_HIVE_SQL_REDUCE_SLOW_GC_RATIO] =
        String.valueOf(slowTaskRatios[BaseValues.TASK_AVG_GC_TIME][1]/taskReduceNums);
    values[BaseValues.DB_COLUMN_HIVE_SQL_REDUCE_SLOW_START_RATIO] =
        String.valueOf(slowTaskRatios[BaseValues.TASK_AVG_START_TIME][1]/taskReduceNums);
    values[BaseValues.DB_COLUMN_HIVE_SQL_REDUCE_READ_DATA_SKEW_RATIO] =
        String.valueOf(slowTaskRatios[BaseValues.TASK_AVG_READ_BYTES][1]/taskReduceNums);
    values[BaseValues.DB_COLUMN_HIVE_SQL_REDUCE_WRITE_DATA_SKEW_RATIO] =
        String.valueOf(slowTaskRatios[BaseValues.TASK_AVG_WRITE_BYTES][1]/taskReduceNums);
    values[BaseValues.DB_COLUMN_HIVE_SQL_READ_BYTES] =
        String.valueOf(readBytes);
    values[BaseValues.DB_COLUMN_HIVE_SQL_WRITE_BYTES] =
        String.valueOf(writeBytes);
    
    hiveSql = "";
    if(conf != null){
      hiveSql = conf.get("hive.query.string");
      //hiveSql = conf.get("yarn.nodemanager.localizer.address");
    }
    values[BaseValues.DB_COLUMN_HIVE_SQL_HIVE_SQL] = hiveSql;
    
    return values;
  }
  
  private long[][] calTaskAvgValues(Map<TaskId, Task> taskInfos, long[] firstStartTime){
    int numCompleteMaps;
    int numCompleteReduces;
    
    long avgMapElapsedTime;
    long avgMapGcTime;
    long avgMapStartTime;
    long avgMapReadBytes;
    long avgMapWriteBytes;
    long avgReduceElapsedTime;
    long avgReduceGcTime;
    long avgReduceStartTime;
    long avgReduceReadBytes;
    long avgReduceWriteBytes;
    long firstMapStartTime;
    long firstReduceStartTime;
    long[][] avgValues;
    
    avgMapElapsedTime = 0;
    avgMapGcTime = 0;
    avgMapStartTime = 0;
    avgMapReadBytes = 0;
    avgMapWriteBytes = 0;
    avgReduceElapsedTime = 0;
    avgReduceGcTime = 0;
    avgReduceStartTime = 0;
    avgReduceReadBytes = 0;
    avgReduceWriteBytes = 0;
    numCompleteMaps = 0;
    numCompleteReduces = 0;
    firstMapStartTime = Long.MAX_VALUE;
    firstReduceStartTime = Long.MAX_VALUE;
    avgValues = new long[BaseValues.TASK_AVG_LEN][2];
    
    for(Task task: taskInfos.values()){
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
      
      if (task.getCounters() != null
          && task.getCounters().getGroup(
              "org.apache.hadoop.mapreduce.FileSystemCounter") != null) {
        readBytes =
            task.getCounters()
                .getGroup("org.apache.hadoop.mapreduce.FileSystemCounter")
                .findCounter("FILE_BYTES_READ").getValue();
        
        writeBytes =
            task.getCounters()
                .getGroup("org.apache.hadoop.mapreduce.FileSystemCounter")
                .findCounter("FILE_BYTES_WRITTEN").getValue();
      }
      
      Map<TaskAttemptId, TaskAttempt> attempts = task.getAttempts();
      for (TaskAttempt attempt : attempts.values()) {
        if(attempt.getState() == TaskAttemptState.SUCCEEDED){
          switch (task.getType()) {
          case MAP:
            numCompleteMaps++;
            avgMapElapsedTime += (attempt.getFinishTime() - attempt.getLaunchTime());
            avgMapGcTime += gcTime;
            avgMapStartTime += attempt.getLaunchTime();
            avgMapReadBytes += readBytes;
            avgMapWriteBytes += writeBytes;
            
            if(attempt.getLaunchTime() < firstMapStartTime){
              firstMapStartTime = attempt.getLaunchTime();
            }
            break;
            
          case REDUCE:
            numCompleteReduces++;
            avgReduceElapsedTime += (attempt.getFinishTime() - attempt.getLaunchTime());
            avgReduceGcTime += gcTime;
            avgReduceStartTime += attempt.getLaunchTime();
            avgReduceReadBytes += readBytes;
            avgReduceWriteBytes += writeBytes;
            
            if(attempt.getLaunchTime() < firstReduceStartTime){
              firstReduceStartTime = attempt.getLaunchTime();
            }
            break;
            
            }
        }
      }
    }
    
    if (numCompleteMaps > 0) {
      tool.addTaskCounter(numCompleteMaps, avgMapElapsedTime, avgMapGcTime,
          avgMapReadBytes, avgMapWriteBytes);

      avgMapElapsedTime /= numCompleteMaps;
      avgMapGcTime /= numCompleteMaps;
      avgMapStartTime /= numCompleteMaps;
      avgMapReadBytes /= numCompleteMaps;
      avgMapWriteBytes /= numCompleteMaps;
    }

    if (numCompleteReduces > 0) {
      tool.addTaskCounter(numCompleteReduces, avgReduceElapsedTime,
          avgReduceGcTime, avgReduceReadBytes, avgReduceWriteBytes);

      avgReduceElapsedTime /= numCompleteReduces;
      avgReduceGcTime /= numCompleteReduces;
      avgReduceStartTime /= numCompleteReduces;
      avgReduceReadBytes /= numCompleteReduces;
      avgReduceWriteBytes /= numCompleteReduces;
    }
    
    firstStartTime[0] = firstMapStartTime;
    firstStartTime[1] = firstReduceStartTime;
    avgValues = new long[BaseValues.TASK_AVG_LEN][2];
    
    avgValues[BaseValues.TASK_AVG_ELAPSED_TIME][0] = avgMapElapsedTime;
    avgValues[BaseValues.TASK_AVG_ELAPSED_TIME][1] = avgReduceElapsedTime;
    avgValues[BaseValues.TASK_AVG_GC_TIME][0] = avgMapGcTime;
    avgValues[BaseValues.TASK_AVG_GC_TIME][1] = avgReduceGcTime;
    avgValues[BaseValues.TASK_AVG_START_TIME][0] = avgMapStartTime;
    avgValues[BaseValues.TASK_AVG_START_TIME][1] = avgReduceStartTime;
    avgValues[BaseValues.TASK_AVG_READ_BYTES][0] = avgMapReadBytes;
    avgValues[BaseValues.TASK_AVG_READ_BYTES][1] = avgReduceReadBytes;
    avgValues[BaseValues.TASK_AVG_WRITE_BYTES][0] = avgMapWriteBytes;
    avgValues[BaseValues.TASK_AVG_WRITE_BYTES][1] = avgReduceWriteBytes;
    
    return avgValues;
  }
  
  private String[] parseTaskInfos(String jobId, String jobName, String user,
      Task task, long[][] avgTaskValues, long[] firstTaskTime) {
    TaskReport report;
    Counters counters;
    String[] values;

    values = new String[BaseValues.DB_COLUMN_JOB_TASK_LEN];
    values[BaseValues.DB_COLUMN_JOB_TASK_JOB_ID] = jobId;
    values[BaseValues.DB_COLUMN_JOB_TASK_JOB_NAME] = jobName;
    values[BaseValues.DB_COLUMN_JOB_TASK_USER_NAME] = user;

    report = task.getReport();
    if (report != null) {
      values[BaseValues.DB_COLUMN_JOB_TASK_TASK_ID] = task.getID().toString();
      values[BaseValues.DB_COLUMN_JOB_TASK_TASK_TYPE] =
          task.getType().toString();
      values[BaseValues.DB_COLUMN_JOB_TASK_TASK_STATUS] =
          report.getTaskState().toString();
      values[BaseValues.DB_COLUMN_JOB_TASK_ELAPSED_TIME] =
          String.valueOf(report.getFinishTime() - report.getStartTime());
      values[BaseValues.DB_COLUMN_JOB_TASK_TASK_ATTEMPT_NUM] =
          String.valueOf(task.getAttempts().size());
      values[BaseValues.DB_COLUMN_JOB_TASK_START_TIME] =
          String.valueOf(report.getStartTime());
      values[BaseValues.DB_COLUMN_JOB_TASK_FINISH_TIME] =
          String.valueOf(report.getFinishTime());

      values[BaseValues.DB_COLUMN_JOB_TASK_GC_TIME] = String.valueOf(0);
      values[BaseValues.DB_COLUMN_JOB_TASK_READ_BYTES] = String.valueOf(0);
      values[BaseValues.DB_COLUMN_JOB_TASK_WRITE_BYTES] = String.valueOf(0);
      
      counters = task.getReport().getCounters();
      if (counters != null) {
        if (counters.getCounter(TaskCounter.GC_TIME_MILLIS) != null) {
          values[BaseValues.DB_COLUMN_JOB_TASK_GC_TIME] =
              String.valueOf(counters.getCounter(TaskCounter.GC_TIME_MILLIS)
                  .getValue());
        }

        if (task.getCounters() != null
            && task.getCounters().getGroup(
                "org.apache.hadoop.mapreduce.FileSystemCounter") != null) {
          values[BaseValues.DB_COLUMN_JOB_TASK_READ_BYTES] =
              String.valueOf(task.getCounters()
                  .getGroup("org.apache.hadoop.mapreduce.FileSystemCounter")
                  .findCounter("FILE_BYTES_READ").getValue());
          
          values[BaseValues.DB_COLUMN_JOB_TASK_WRITE_BYTES] =
              String.valueOf(task.getCounters()
                  .getGroup("org.apache.hadoop.mapreduce.FileSystemCounter")
                  .findCounter("FILE_BYTES_WRITTEN").getValue());
        }
        
        values[BaseValues.DB_COLUMN_JOB_TASK_IS_SLOW_RUNNING] = "0";
        values[BaseValues.DB_COLUMN_JOB_TASK_IS_SLOW_GC] = "0";
        values[BaseValues.DB_COLUMN_JOB_TASK_IS_SLOW_START] = "0";
        values[BaseValues.DB_COLUMN_JOB_TASK_IS_READ_DATA_SKEW] = "0";
        values[BaseValues.DB_COLUMN_JOB_TASK_IS_WRITE_DATA_SKEW] = "0";

        values[BaseValues.DB_COLUMN_JOB_TASK_AVG_ELAPSED_TIME] = "0";
        values[BaseValues.DB_COLUMN_JOB_TASK_AVG_GC_TIME] = "0";
        values[BaseValues.DB_COLUMN_JOB_TASK_AVG_START_TIME] = "0";
        values[BaseValues.DB_COLUMN_JOB_TASK_AVG_READ_BYTES] = "0";
        values[BaseValues.DB_COLUMN_JOB_TASK_AVG_WRITE_BYTES] = "0";

        if (task.getType() == TaskType.MAP) {
          values[BaseValues.DB_COLUMN_JOB_TASK_AVG_ELAPSED_TIME] =
              String
                  .valueOf(avgTaskValues[BaseValues.TASK_AVG_ELAPSED_TIME][0]);
          values[BaseValues.DB_COLUMN_JOB_TASK_AVG_GC_TIME] =
              String.valueOf(avgTaskValues[BaseValues.TASK_AVG_GC_TIME][0]);
          values[BaseValues.DB_COLUMN_JOB_TASK_AVG_START_TIME] =
              String.valueOf(avgTaskValues[BaseValues.TASK_AVG_START_TIME][0]);
          values[BaseValues.DB_COLUMN_JOB_TASK_AVG_READ_BYTES] =
              String.valueOf(avgTaskValues[BaseValues.TASK_AVG_READ_BYTES][0]);
          values[BaseValues.DB_COLUMN_JOB_TASK_AVG_WRITE_BYTES] =
              String.valueOf(avgTaskValues[BaseValues.TASK_AVG_WRITE_BYTES][0]);

          if (avgTaskValues[BaseValues.TASK_AVG_ELAPSED_TIME][0]
              * (1 + slowRatio) <= (report.getFinishTime() - report
              .getStartTime())) {
            values[BaseValues.DB_COLUMN_JOB_TASK_IS_SLOW_RUNNING] = "1";
          }

          if (avgTaskValues[BaseValues.TASK_AVG_GC_TIME][0] * (1 + slowRatio) <= Long
              .parseLong(values[BaseValues.DB_COLUMN_JOB_TASK_GC_TIME])) {
            values[BaseValues.DB_COLUMN_JOB_TASK_IS_SLOW_GC] = "1";
          }

          if ((avgTaskValues[BaseValues.TASK_AVG_START_TIME][0] - firstTaskTime[0])
              * (1 + slowRatio) <= (report.getStartTime() - firstTaskTime[0])) {
            values[BaseValues.DB_COLUMN_JOB_TASK_IS_SLOW_START] = "1";
          }
          
          if (avgTaskValues[BaseValues.TASK_AVG_READ_BYTES][0]
              * (1 + slowRatio) <= Long
                .parseLong(values[BaseValues.DB_COLUMN_JOB_TASK_READ_BYTES])) {
            values[BaseValues.DB_COLUMN_JOB_TASK_IS_READ_DATA_SKEW] = "1";
          }

          if (avgTaskValues[BaseValues.TASK_AVG_WRITE_BYTES][0]
              * (1 + slowRatio) <= Long
                .parseLong(values[BaseValues.DB_COLUMN_JOB_TASK_WRITE_BYTES])) {
            values[BaseValues.DB_COLUMN_JOB_TASK_IS_WRITE_DATA_SKEW] = "1";
          }
        } else if (task.getType() == TaskType.REDUCE) {
          values[BaseValues.DB_COLUMN_JOB_TASK_AVG_ELAPSED_TIME] =
              String
                  .valueOf(avgTaskValues[BaseValues.TASK_AVG_ELAPSED_TIME][1]);
          values[BaseValues.DB_COLUMN_JOB_TASK_AVG_GC_TIME] =
              String.valueOf(avgTaskValues[BaseValues.TASK_AVG_GC_TIME][1]);
          values[BaseValues.DB_COLUMN_JOB_TASK_AVG_START_TIME] =
              String.valueOf(avgTaskValues[BaseValues.TASK_AVG_START_TIME][1]);
          values[BaseValues.DB_COLUMN_JOB_TASK_AVG_READ_BYTES] =
              String.valueOf(avgTaskValues[BaseValues.TASK_AVG_READ_BYTES][1]);
          values[BaseValues.DB_COLUMN_JOB_TASK_AVG_WRITE_BYTES] =
              String.valueOf(avgTaskValues[BaseValues.TASK_AVG_WRITE_BYTES][1]);

          if (avgTaskValues[BaseValues.TASK_AVG_ELAPSED_TIME][1]
              * (1 + slowRatio) <= (report.getFinishTime() - report
              .getStartTime())) {
            values[BaseValues.DB_COLUMN_JOB_TASK_IS_SLOW_RUNNING] = "1";
          }

          if (avgTaskValues[BaseValues.TASK_AVG_GC_TIME][1] * (1 + slowRatio) <= Long
              .parseLong(values[BaseValues.DB_COLUMN_JOB_TASK_GC_TIME])) {
            values[BaseValues.DB_COLUMN_JOB_TASK_IS_SLOW_GC] = "1";
          }

          if ((avgTaskValues[BaseValues.TASK_AVG_START_TIME][1] - firstTaskTime[1])
              * (1 + slowRatio) <= (report.getStartTime() - firstTaskTime[1])) {
            values[BaseValues.DB_COLUMN_JOB_TASK_IS_SLOW_START] = "1";
          }
          
          if (avgTaskValues[BaseValues.TASK_AVG_READ_BYTES][1]
              * (1 + slowRatio) <= Long
                .parseLong(values[BaseValues.DB_COLUMN_JOB_TASK_READ_BYTES])) {
            values[BaseValues.DB_COLUMN_JOB_TASK_IS_READ_DATA_SKEW] = "1";
          }

          if (avgTaskValues[BaseValues.TASK_AVG_WRITE_BYTES][1]
              * (1 + slowRatio) <= Long
                .parseLong(values[BaseValues.DB_COLUMN_JOB_TASK_WRITE_BYTES])) {
            values[BaseValues.DB_COLUMN_JOB_TASK_IS_WRITE_DATA_SKEW] = "1";
          }
        }
      }
    }

    return values;
  }
  
  private String[] parseTaskAttemptInfos(String jobId, String jobName,
      String user, TaskAttempt taskAttempt, TaskType taskType, long[][] avgTaskValues) {
    String[] values;
    
    values = new String[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_LEN];
    
    long elapsedTime =
        taskAttempt.getFinishTime() - taskAttempt.getLaunchTime();
    values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_ERROR_TYPE] =
        String.valueOf(BaseValues.TASK_ATTEMPT_ERROR_TYPE_NONE);
    
    if (taskType == TaskType.MAP
        && avgTaskValues[BaseValues.TASK_AVG_ELAPSED_TIME][0] * (1 + slowRatio) <= elapsedTime) {
      values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_ERROR_TYPE] =
          String.valueOf(BaseValues.TASK_ATTEMPT_ERROR_TYPE_SLOW_ELAPSED);
    } else if (taskType == TaskType.REDUCE
        && avgTaskValues[BaseValues.TASK_AVG_ELAPSED_TIME][1] * (1 + slowRatio) <= elapsedTime) {
      values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_ERROR_TYPE] =
          String.valueOf(BaseValues.TASK_ATTEMPT_ERROR_TYPE_SLOW_ELAPSED);
    }
    
    if (!values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_ERROR_TYPE].equals(String
        .valueOf(BaseValues.TASK_ATTEMPT_ERROR_TYPE_SLOW_ELAPSED))
        && taskAttempt.getState() == TaskAttemptState.SUCCEEDED) {
      return null;
    }
    
    long readBytes;
    long writeBytes;
    
    readBytes = 0;
    writeBytes = 0;
    if (taskAttempt.getCounters() != null
        && taskAttempt.getCounters().getGroup(
            "org.apache.hadoop.mapreduce.FileSystemCounter") != null) {
      readBytes =
          taskAttempt.getCounters()
              .getGroup("org.apache.hadoop.mapreduce.FileSystemCounter")
              .findCounter("FILE_BYTES_READ").getValue();
      
      writeBytes =
          taskAttempt.getCounters()
              .getGroup("org.apache.hadoop.mapreduce.FileSystemCounter")
              .findCounter("FILE_BYTES_WRITTEN").getValue();
    }
    
    // judge if data skew
    if (taskType == TaskType.MAP) {
      if (avgTaskValues[BaseValues.TASK_AVG_READ_BYTES][0] * (1 + slowRatio) <= readBytes) {
        values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_ERROR_TYPE] =
            String
                .valueOf(BaseValues.TASK_ATTEMPT_ERROR_TYPE_SLOW_ELAPSED_AND_READ_DATA_SKEW);
      }

      if (avgTaskValues[BaseValues.TASK_AVG_WRITE_BYTES][0] * (1 + slowRatio) <= writeBytes) {
        if (values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_ERROR_TYPE]
            .equals(String
                .valueOf(BaseValues.TASK_ATTEMPT_ERROR_TYPE_SLOW_ELAPSED_AND_READ_DATA_SKEW))) {
          values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_ERROR_TYPE] =
              String
                  .valueOf(BaseValues.TASK_ATTEMPT_ERROR_TYPE_SLOW_ELAPSED_AND_ALL_DATA_SKEW);
        } else {
          values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_ERROR_TYPE] =
              String
                  .valueOf(BaseValues.TASK_ATTEMPT_ERROR_TYPE_SLOW_ELAPSED_AND_WRITE_DATA_SKEW);
        }
      }
    } else if (taskType == TaskType.REDUCE) {
      if (avgTaskValues[BaseValues.TASK_AVG_READ_BYTES][1] * (1 + slowRatio) <= readBytes) {
        values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_ERROR_TYPE] =
            String
                .valueOf(BaseValues.TASK_ATTEMPT_ERROR_TYPE_SLOW_ELAPSED_AND_READ_DATA_SKEW);
      }

      if (avgTaskValues[BaseValues.TASK_AVG_WRITE_BYTES][1] * (1 + slowRatio) <= writeBytes) {

        if (values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_ERROR_TYPE]
            .equals(String
                .valueOf(BaseValues.TASK_ATTEMPT_ERROR_TYPE_SLOW_ELAPSED_AND_READ_DATA_SKEW))) {
          values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_ERROR_TYPE] =
              String
                  .valueOf(BaseValues.TASK_ATTEMPT_ERROR_TYPE_SLOW_ELAPSED_AND_ALL_DATA_SKEW);
        } else {
          values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_ERROR_TYPE] =
              String
                  .valueOf(BaseValues.TASK_ATTEMPT_ERROR_TYPE_SLOW_ELAPSED_AND_WRITE_DATA_SKEW);
        }
      }
    }
    
    values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_JOB_ID] = jobId;
    values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_JOB_NAME] = jobName;
    values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_USER_NAME] = user;
    values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_ID] =
        taskAttempt.getID().toString();
    values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_STATUS] =
        taskAttempt.getState().toString();
    values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_TYPE] = taskType.toString();
    values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_RACK_NAME] =
        taskAttempt.getNodeRackName();
    values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_ELAPSED_TIME] =
        String.valueOf(taskAttempt.getFinishTime()
            - taskAttempt.getLaunchTime());
    values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_NOTE] =
        taskAttempt.getReport().getDiagnosticInfo();
    values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_START_TIME] =
        String.valueOf(taskAttempt.getLaunchTime());
    values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_FINISH_TIME] =
        String.valueOf(taskAttempt.getFinishTime());
    
    if (taskAttempt.getNodeHttpAddress() != null) {
      values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_ASSIGN_NODE] =
          taskAttempt.getNodeHttpAddress().split(":")[0];
    } else {
      values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_ASSIGN_NODE] = "unknow";
    }
    
    String noteInfo = values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_NOTE];
    
    if (noteInfo.contains("Speculation")) {
      values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_ERROR_TYPE] =
          String.valueOf(BaseValues.TASK_ATTEMPT_ERROR_TYPE_SPECULATION);
    } else if (noteInfo.contains("ShuffleError")) {
      values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_ERROR_TYPE] =
          String.valueOf(BaseValues.TASK_ATTEMPT_ERROR_TYPE_SHUFFLE_ERROR);
    } else if (noteInfo.contains("Reducer preempted")) {
      values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_ERROR_TYPE] =
          String.valueOf(BaseValues.TASK_ATTEMPT_ERROR_TYPE_REDUCE_PREEMPTED);
    } else if (noteInfo.contains("Timed out")) {
      values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_ERROR_TYPE] =
          String.valueOf(BaseValues.TASK_ATTEMPT_ERROR_TYPE_TIME_OUT);
    } else if (noteInfo.contains("Task KILL is received")) {
      values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_ERROR_TYPE] =
          String.valueOf(BaseValues.TASK_ATTEMPT_ERROR_TYPE_KILL_RECEIVED);
    } else if (noteInfo != null && !noteInfo.equals("")) {
      values[BaseValues.DB_COLUMN_JOB_TASK_ATTEMPT_ERROR_TYPE] =
          String.valueOf(BaseValues.TASK_ATTEMPT_ERROR_TYPE_OTHERS);
    }
    
    return values;
  }

}
