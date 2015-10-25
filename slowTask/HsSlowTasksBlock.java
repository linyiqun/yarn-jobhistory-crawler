package org.apache.hadoop.mapreduce.v2.hs.webapp;

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.SLOW_TASKS_RATIO;
import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.SLOW_TASKS_TYPE;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.v2.api.records.Counter;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobInfo;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.InputType;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

public class HsSlowTasksBlock extends HtmlBlock{
	final AppContext appContext;
	final SimpleDateFormat dateFormat =
		    new SimpleDateFormat("yyyy.MM.dd HH:mm:ss z");

	  @Inject HsSlowTasksBlock(AppContext appctx) {
	    appContext = appctx;
	  }
	
	@Override
	protected void render(Block html) {
		// TODO Auto-generated method stub
		TBODY<TABLE<Hamlet>> tbody = html.
			      h2("Retired Tasks").
			      table("#jobs").
			        thead().
			        tr().
		          th().input("slow_tasks_ratio").$type(InputType.text).$name("start_time").$value("input ratio")._()._().
		          th().input("search_confirm").$type(InputType.button).$name("search").$value("SlowTask Search").$onclick("slowtasksearch()")._()._()._().
			          tr().
			          th("Submit Time").
                th("Start Time").
                th("Finish Time").
                th(".id", "Job ID").
                th(".name", "Name").
                th("User").
                th("Task Start Time").
                th("Task Finish Time").
                th("Task Avg StartTime").
                th("Task ID").
                th("Task Type").
                th("Task State").
                th("Elapsed Time").
                th("Avg Elapsed Time").
                th("Gc Count").
                th("Gc Time").
                th("Read Bytes").
                th("Write Bytes").
                th("Avg Read Bytes").
                th("Avg Write Bytes").
                th("Avg Gc Time").
                th("Cpu Useage")._()._().
			        tbody();
		
		Map<TaskId, Task> taskMap;
		Task t;
		TaskReport report;
		Counter gcCounter;
		Counter gcTimeCounter;
		Counter cpuUseageCounter;
		Counter bytesReadCounter;
		Counter bytesWriteCounter;
		Counters counters;
		
		StringBuilder jobPrefixInfo;
		double slowRatio;
		
		StringBuilder jobsTableData = new StringBuilder("[\n");
	    for (Job j : appContext.getAllJobs().values()) {
	    	JobId jobId = j.getID();
	    	Job jb = appContext.getJob(jobId);
	    	JobInfo job = new JobInfo(jb);
	    	
	    	taskMap = null;
	    	slowRatio = Double.parseDouble($(SLOW_TASKS_RATIO));
	    	if($(SLOW_TASKS_TYPE).equals("elapsedtime")){
	    	  taskMap = job.getElapsedSlowTasks(jb, slowRatio);
	    	}else if($(SLOW_TASKS_TYPE).equals("gctime")){
	    	  taskMap = job.getGcSlowTasks(jb, slowRatio);
	    	}else if ($(SLOW_TASKS_TYPE).equals("slowstart")) {
	    	  taskMap = job.getSlowStartTasks(jb, slowRatio);
	    	}else if ($(SLOW_TASKS_TYPE).equals("readdatalean")) {
	    	  taskMap = job.getReadDataLeanTasks(jb, slowRatio);
        }else if ($(SLOW_TASKS_TYPE).equals("writedatalean")) {
          taskMap = job.getWriteDataLeanTasks(jb, slowRatio);
        }
	        
      jobPrefixInfo = new StringBuilder();
      jobPrefixInfo
          .append(dateFormat.format(new Date(job.getSubmitTime())))
          .append("\",\"")
          .append(dateFormat.format(new Date(job.getStartTime())))
          .append("\",\"")
          .append(dateFormat.format(new Date(job.getFinishTime())))
          .append("\",\"")
          .append("<a href='")
          .append(url("job", job.getId()))
          .append("'>")
          .append(job.getId())
          .append("</a>\",\"")
          .append(
              StringEscapeUtils.escapeJavaScript(StringEscapeUtils
                  .escapeHtml(job.getName())))
          .append("\",\"")
          .append(
              StringEscapeUtils.escapeJavaScript(StringEscapeUtils
                  .escapeHtml(job.getUserName()))).append("\",\"");

      
	      for(Entry<TaskId, Task> entry: taskMap.entrySet()){
	    	  t = entry.getValue();
	    	  long gcCount = -1;
	    	  long bytesRead = 0;
	    	  long bytesWrite = 0;
	    	  long gcTotalTime = -1;
	    	  
	    	  double cpuUseage = -1;
	    	  
	    	  report = t.getReport();
	    	  
	    	  gcCounter = null;
	    	  gcTimeCounter = null;
	    	  cpuUseageCounter = null;
	    	  bytesReadCounter = null;
	    	  bytesWriteCounter = null;
	    	  
        if (report.getCounters() != null) {
          counters = report.getCounters();

          gcCounter = counters.getCounter(TaskCounter.GC_COUNTERS);
          gcTimeCounter = counters.getCounter(TaskCounter.GC_TIME_MILLIS);
          cpuUseageCounter =
              counters.getCounter(TaskCounter.CPU_USAGE_PERCENTS);
          bytesReadCounter = counters.getCounter(FileSystemCounter.BYTES_READ);
          bytesWriteCounter =
              counters.getCounter(FileSystemCounter.BYTES_WRITTEN);
        }

        if (gcCounter != null) {
          gcCount = gcCounter.getValue();
        }

        if (gcTimeCounter != null) {
          gcTotalTime = gcTimeCounter.getValue();
        }

        if (cpuUseageCounter != null) {
          cpuUseage = cpuUseageCounter.getValue();
        }

        if (bytesReadCounter != null) {
          bytesRead = bytesReadCounter.getValue();
        }

        if (bytesWriteCounter != null) {
          bytesWrite = bytesWriteCounter.getValue();
        }
	    	  
	    	  jobsTableData.append("[\"")
	    	  .append(jobPrefixInfo)
		      .append(dateFormat.format(new Date(report.getStartTime()))).append("\",\"")
		      .append(dateFormat.format(new Date(report.getFinishTime()))).append("\",\"")
		      .append(dateFormat.format(new Date(t.getType() == TaskType.MAP ? job.getAvgMapStartTime() : job.getAvgReduceStartTime()))).append("\",\"")
		      .append(t.getID()).append("\",\"")
		      .append(t.getType().toString()).append("\",\"")
		      .append(report.getTaskState().toString()).append("\",\"")
		      .append(String.valueOf(secToTime(((report.getFinishTime()-report.getStartTime())/1000)))).append("\",\"")
		      .append(String.valueOf(secToTime((t.getType() == TaskType.MAP ? job.getAvgMapElapsedTime() : job.getAvgReduceElapsedTime())/1000))).append("\",\"")
		      .append(String.valueOf(gcCount)).append("\",\"")
		      .append(gcTotalTime).append("\",\"")
		      .append(t.getType() == TaskType.MAP ? job.getAvgMapGcTime() : job.getAvgReduceGcTime()).append("\",\"")
		      .append(bytesRead).append("\",\"")
		      .append(bytesWrite).append("\",\"")
		      .append(t.getType() == TaskType.MAP ? job.getAvgMapReadBytes() : job.getAvgReduceReadBytes()).append("\",\"")
		      .append(t.getType() == TaskType.MAP ? job.getAvgMapWriteBytes() : job.getAvgReduceWriteBytes()).append("\",\"")
		      .append(String.valueOf(cpuUseage)).append("\"],\n");
	      }
	    }

	    //Remove the last comma and close off the array of arrays
	    if(jobsTableData.charAt(jobsTableData.length() - 2) == ',') {
	      jobsTableData.delete(jobsTableData.length()-2, jobsTableData.length()-1);
	    }
	    jobsTableData.append("]");
	    
	    String onClickMethods = 
          "function slowtasksearch() {\n"+
              "    var ratio = $('.slow_tasks_ratio').val()\n"+
              "    window.location ='/jobhistory/slowtasks/" + $(SLOW_TASKS_TYPE) + "/' + ratio\n"+
              "}\n";
	    html.script().$type("text/javascript").
	    _("var jobsTableData=" + jobsTableData + "\n" + onClickMethods)._();
	    
	    tbody._().
	    tfoot().
	      tr().
	        th().input("search_init").$type(InputType.text).$name("submit_time").$value("Submit Time")._()._().
	        th().input("search_init").$type(InputType.text).$name("start_time").$value("Start Time")._()._().
	        th().input("search_init").$type(InputType.text).$name("finish_time").$value("Finish Time")._()._().
	        th().input("search_init").$type(InputType.text).$name("job_id").$value("Job ID")._()._().
	        th().input("search_init").$type(InputType.text).$name("name").$value("Name")._()._().
	        th().input("search_init").$type(InputType.text).$name("user").$value("User")._()._().
	        th().input("search_init").$type(InputType.text).$name("task_start_time").$value("Task StartTime")._()._().
	        th().input("search_init").$type(InputType.text).$name("task_finish_time").$value("Task FinishTime")._()._().
	        th().input("search_init").$type(InputType.text).$name("task_avg_starttime").$value("Task Avg StartTime")._()._().
	        th().input("search_init").$type(InputType.text).$name("task_id").$value("Task ID")._()._().
	        th().input("search_init").$type(InputType.text).$name("task_type").$value("Task Type")._()._().
	        th().input("search_init").$type(InputType.text).$name("task_state").$value("Task State")._()._().
	        th().input("search_init").$type(InputType.text).$name("elapsed_time").$value("Elapsed Time")._()._().
	        th().input("search_init").$type(InputType.text).$name("avg_elapsed_time").$value("Avg Elapsed Time")._()._().
	        th().input("search_init").$type(InputType.text).$name("gc_count").$value("Gc Count")._()._().
	        th().input("search_init").$type(InputType.text).$name("gc_time").$value("Gc Time")._()._().
	        th().input("search_init").$type(InputType.text).$name("gc_avg_time").$value("Gc Avg Time")._()._().
	        th().input("search_init").$type(InputType.text).$name("cpu_useage").$value("Cpu Useage")._()._().
	        _().
	      _().
	    _();
	} 
	
  public static String secToTime(long time) {
    String timeStr = null;
    long hour = 0;
    long minute = 0;
    long second = 0;
    
    if (time <= 0)
      return "00:00:00";
    else {
      minute = time / 60;
      if (minute < 60) {
        second = time % 60;
        timeStr = "00:" + unitFormat(minute) + ":" + unitFormat(second);
      } else {
        hour = minute / 60;
        if (hour > 99)
          return "99:59:59";
        minute = minute % 60;
        second = time - hour * 3600 - minute * 60;
        timeStr =
            unitFormat(hour) + ":" + unitFormat(minute) + ":"
                + unitFormat(second);
      }
    }
    return timeStr;
  }

  public static String unitFormat(long i) {
    String retStr = null;
    if (i >= 0 && i < 10)
      retStr = "0" + Long.toString(i);
    else
      retStr = "" + i;
    return retStr;
  }

}
