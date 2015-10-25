package org.apache.hadoop.mapreduce.v2.hs.webapp;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobInfo;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.InputType;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

public class HsSlowTaskAttemptsBlock extends HtmlBlock{
	final AppContext appContext;
	final SimpleDateFormat dateFormat =
		    new SimpleDateFormat("yyyy.MM.dd HH:mm:ss z");

	  @Inject HsSlowTaskAttemptsBlock(AppContext appctx) {
	    appContext = appctx;
	  }
	
	@Override
	protected void render(Block html) {
		// TODO Auto-generated method stub
		TBODY<TABLE<Hamlet>> tbody = html.
			      h2("Retired TaskAttempts").
			      table("#jobs").
			        thead().
			          tr().
			          th("Submit Time").
                th("Start Time").
                th("Finish Time").
                th(".id", "Job ID").
                th(".name", "Name").
                th("User").
                th("TaskAttempt Start Time").
                th("TaskAttempt Finish Time").
                th("TaskAttempt ID").
                th("TaskAttempt Type").
                th("TaskAttempt State").
                th("HttpAddress").
                th("Elapsed Time").
                th("Avg MapTime").
                th("Avg MergeTime").
                th("Avg ShuffleTime").
                th("Avg ReduceTime").
                th("Note")._()._().
			        tbody();
		
		Task t;
		TaskType type;
		TaskAttempt taskAttempt;
		Map<TaskId, Task> taskMap;
		Map<TaskAttemptId, TaskAttempt> taskAttempts;
		
		long mapTime;
		long mergeTime;
		long shuffleTime;
		long reduceTime;
		
		StringBuilder jobPrefixInfo;
		
		StringBuilder jobsTableData = new StringBuilder("[\n");
	    for (Job j : appContext.getAllJobs().values()) {
	      JobId jobId = j.getID();
        Job jb = appContext.getJob(jobId);
        JobInfo job = new JobInfo(jb);
        taskMap = jb.getTasks();
        
        jobPrefixInfo = new StringBuilder();
        jobPrefixInfo.append(dateFormat.format(new Date(job.getSubmitTime()))).append("\",\"")
        .append(dateFormat.format(new Date(job.getStartTime()))).append("\",\"")
        .append(dateFormat.format(new Date(job.getFinishTime()))).append("\",\"")
        .append("<a href='").append(url("job", job.getId())).append("'>")
        .append(job.getId()).append("</a>\",\"")
        .append(StringEscapeUtils.escapeJavaScript(StringEscapeUtils.escapeHtml(
          job.getName()))).append("\",\"")
        .append(StringEscapeUtils.escapeJavaScript(StringEscapeUtils.escapeHtml(
          job.getUserName()))).append("\",\"");
	      
	      for(Entry<TaskId, Task> entry: taskMap.entrySet()){
	        t = entry.getValue();
	        taskAttempts = t.getAttempts();
          type = t.getType();
          
	        for(Entry<TaskAttemptId, TaskAttempt> ta: taskAttempts.entrySet()){
	          taskAttempt = ta.getValue();
	          
	          if(taskAttempt.getState() == TaskAttemptState.SUCCEEDED){
	            continue;
	          }
	          
          if (type == TaskType.MAP) {
            mapTime = taskAttempt.getFinishTime() - taskAttempt.getLaunchTime();
            shuffleTime = 0;
            mergeTime = 0;
            reduceTime = 0;
          } else {
            mapTime = 0;
            shuffleTime =
                taskAttempt.getShuffleFinishTime()
                    - taskAttempt.getLaunchTime();
            mergeTime =
                taskAttempt.getSortFinishTime()
                    - taskAttempt.getShuffleFinishTime();
            reduceTime =
                taskAttempt.getFinishTime() - taskAttempt.getSortFinishTime();
          }
	          
	          jobsTableData.append("[\"")
	          .append(jobPrefixInfo)
	          .append(dateFormat.format(new Date(taskAttempt.getLaunchTime()))).append("\",\"")
	          .append(dateFormat.format(new Date(taskAttempt.getFinishTime()))).append("\",\"")
	          .append(taskAttempt.getID()).append("\",\"")
	          .append(type).append("\",\"")
	          .append(taskAttempt.getState()).append("\",\"")
	          .append(taskAttempt.getNodeHttpAddress()).append("\",\"")
	          .append(String.valueOf(secToTime(((taskAttempt.getFinishTime()-taskAttempt.getLaunchTime())/1000)))).append("\",\"")
	          .append(String.valueOf(secToTime((mapTime/1000)))).append("\",\"")
	          .append(String.valueOf(secToTime((mergeTime/1000)))).append("\",\"")
	          .append(String.valueOf(secToTime((shuffleTime/1000)))).append("\",\"")
	          .append(String.valueOf(secToTime((reduceTime/1000)))).append("\",\"")
	          .append(StringEscapeUtils.escapeJavaScript(StringEscapeUtils.escapeHtml(taskAttempt.getReport().getDiagnosticInfo()))).append("\"],\n");
	        }
	      }
	    }

	    //Remove the last comma and close off the array of arrays
	    if(jobsTableData.charAt(jobsTableData.length() - 2) == ',') {
	      jobsTableData.delete(jobsTableData.length()-2, jobsTableData.length()-1);
	    }
	    jobsTableData.append("]");
	    
	    html.script().$type("text/javascript").
      _("var jobsTableData=" + jobsTableData)._();
	    tbody._().
	    tfoot().
	      tr().
	        th().input("search_init").$type(InputType.text).$name("submit_time").$value("Submit Time")._()._().
	        th().input("search_init").$type(InputType.text).$name("start_time").$value("Start Time")._()._().
	        th().input("search_init").$type(InputType.text).$name("finish_time").$value("Finish Time")._()._().
	        th().input("search_init").$type(InputType.text).$name("job_id").$value("Job Id")._()._().
	        th().input("search_init").$type(InputType.text).$name("name").$value("Name")._()._().
	        th().input("search_init").$type(InputType.text).$name("user").$value("User")._()._().
	        th().input("search_init").$type(InputType.text).$name("taskattempt_start_time").$value("TaskAttempt StartTime")._()._().
	        th().input("search_init").$type(InputType.text).$name("taskattempt_finish_time").$value("TaskAttempt FinishTime")._()._().
	        th().input("search_init").$type(InputType.text).$name("taskattempt_id").$value("TaskAttempt ID")._()._().
	        th().input("search_init").$type(InputType.text).$name("taskattempt_type").$value("TaskAttempt Type")._()._().
	        th().input("search_init").$type(InputType.text).$name("taskattempt_state").$value("TaskAttempt State")._()._().
	        th().input("search_init").$type(InputType.text).$name("httpaddress").$value("HttpAddress")._()._().
	        th().input("search_init").$type(InputType.text).$name("elapsed_time").$value("Elapsed Time")._()._().
	        th().input("search_init").$type(InputType.text).$name("avg_maptime").$value("Avg MapTime")._()._().
	        th().input("search_init").$type(InputType.text).$name("avg_mergetime").$value("Avg MergeTime")._()._().
	        th().input("search_init").$type(InputType.text).$name("avg_shuffletime").$value("Avg ShuffleTime")._()._().
	        th().input("search_init").$type(InputType.text).$name("avg_reducetime").$value("Avg ReduceTime")._()._().
	        th().input("search_init").$type(InputType.text).$name("note").$value("Note")._()._().
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
