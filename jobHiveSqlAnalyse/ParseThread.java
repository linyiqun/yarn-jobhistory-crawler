package org.apache.hadoop.mapreduce.v2.hs.tool.sqlanalyse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ParseThread extends Thread{
	private HiveSqlAnalyseTool tool;
	private LinkedList<FileStatus> fileStatus;
	private HashMap<String, String[]> dataInfos;
	
	public ParseThread(HiveSqlAnalyseTool tool, LinkedList<FileStatus> fileStatus, HashMap<String, String[]> dataInfos){
		this.tool = tool;
		this.fileStatus = fileStatus;
		this.dataInfos = dataInfos;
	}
	
	@Override
	public void run() {
		FileStatus fs;
		
		while(fileStatus != null && !fileStatus.isEmpty()){
			fs = tool.getOneFile();
			parseFileInfo(fs);
		}
		
		super.run();
	}

	private void parseFileInfo(FileStatus fs) {
		String str;
		String username;
		String fileType;
		String jobId;
		String jobName;
		String hiveSql;

		int startPos;
		int endPos;
		int hiveSqlFlag;
		long launchTime;
		long finishTime;
		int mapTaskNum;
		int reduceTaskNum;
		String xmlNameFlag;
		String launchTimeFlag;
		String finishTimeFlag;
		String launchMapFlag;
		String launchReduceFlag;

		Path path;
		FileSystem fileSystem;
		InputStream in;

		fileType = "";
		hiveSql = "";
		jobId = "";
		jobName = "";
		username = "";
		hiveSqlFlag = 0;
		launchTime = 0;
		finishTime = 0;
		mapTaskNum = 0;
		reduceTaskNum = 0;
		xmlNameFlag = "<value>";
		launchTimeFlag = "\"launchTime\":";
		finishTimeFlag = "\"finishTime\":";
		launchMapFlag = "\"Launched map tasks\"";
		launchReduceFlag = "\"Launched reduce tasks\"";

		path = fs.getPath();
		str = path.getName();
		if (str.endsWith(".xml")) {
			fileType = "config";

			endPos = str.lastIndexOf("_");
			jobId = str.substring(0, endPos);
		} else if (str.endsWith(".jhist")) {
			fileType = "info";

			endPos = str.indexOf("-");
			jobId = str.substring(0, endPos);
		}else{
			return;
		}

		try {
			fileSystem = path.getFileSystem(new Configuration());
			in = fileSystem.open(path);
			InputStreamReader isr;
			BufferedReader br;

			isr = new InputStreamReader(in, "UTF-8");
			br = new BufferedReader(isr);

			while ((str = br.readLine()) != null) {
				if (str.contains("mapreduce.job.user.name")) {
					startPos = str.indexOf(xmlNameFlag);
					endPos = str.indexOf("</value>");
					username = str.substring(startPos + xmlNameFlag.length(),
							endPos);
				} else if (str.contains("mapreduce.job.name")) {
					startPos = str.indexOf(xmlNameFlag);
					endPos = str.indexOf("</value>");
					jobName = str.substring(startPos + xmlNameFlag.length(),
							endPos);
				} else if (str.contains("hive.query.string")) {
					hiveSqlFlag = 1;

					hiveSql = str;
				} else if (hiveSqlFlag == 1) {
					hiveSql += str;

					if (str.contains("</value>")) {
						startPos = hiveSql.indexOf(xmlNameFlag);
						endPos = hiveSql.indexOf("</value>");
						hiveSql = hiveSql.substring(
								startPos + xmlNameFlag.length(), endPos);

						hiveSqlFlag = 0;
					}
				} else if (str.startsWith("{\"type\":\"JOB_INITED\"")) {
					startPos = str.indexOf(launchTimeFlag);
					str = str.substring(startPos + launchTimeFlag.length());
					endPos = str.indexOf(",");
					launchTime = Long.parseLong(str.substring(0, endPos));
				} else if (str.startsWith("{\"type\":\"JOB_FINISHED\"")) {
					mapTaskNum = parseTaskNum(launchMapFlag, str);
					reduceTaskNum = parseTaskNum(launchReduceFlag, str);

					startPos = str.indexOf(finishTimeFlag);
					str = str.substring(startPos + finishTimeFlag.length());
					endPos = str.indexOf(",");
					finishTime = Long.parseLong(str.substring(0, endPos));
				}
			}

			/*System.out.println("jobId is " + jobId);
			System.out.println("jobName is " + jobName);
			System.out.println("username is " + username);
			System.out.println("map task num is " + mapTaskNum);
			System.out.println("reduce task num is " + reduceTaskNum);
			System.out.println("launchTime is " + launchTime);
			System.out.println("finishTime is " + finishTime);
			System.out.println("hive query sql is " + hiveSql);*/
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (fileType.equals("config")) {
			insertConfParseData(jobId, jobName, username, hiveSql);
		} else if (fileType.equals("info")) {
			insertJobInfoParseData(jobId, launchTime, finishTime, mapTaskNum,
					reduceTaskNum);
		}
	}

	private void insertConfParseData(String jobId, String jobName,
			String username, String sql) {
		String[] array;

		if (dataInfos.containsKey(jobId)) {
			array = dataInfos.get(jobId);
		} else {
			array = new String[BaseValues.DB_COLUMN_HIVE_SQL_LEN];
		}

		array[BaseValues.DB_COLUMN_HIVE_SQL_JOBID] = jobId;
		array[BaseValues.DB_COLUMN_HIVE_SQL_JOBNAME] = jobName;
		array[BaseValues.DB_COLUMN_HIVE_SQL_USERNAME] = username;
		array[BaseValues.DB_COLUMN_HIVE_SQL_HIVE_SQL] = sql;

		tool.addDataToMap(jobId, array);
	}

	private void insertJobInfoParseData(String jobId, long launchTime,
			long finishedTime, int mapTaskNum, int reduceTaskNum) {
		String[] array;

		if (dataInfos.containsKey(jobId)) {
			array = dataInfos.get(jobId);
		} else {
			array = new String[BaseValues.DB_COLUMN_HIVE_SQL_LEN];
		}

		array[BaseValues.DB_COLUMN_HIVE_SQL_JOBID] = jobId;
		array[BaseValues.DB_COLUMN_HIVE_SQL_START_TIME] = String
				.valueOf(launchTime);
		array[BaseValues.DB_COLUMN_HIVE_SQL_FINISH_TIME] = String
				.valueOf(finishedTime);
		array[BaseValues.DB_COLUMN_HIVE_SQL_MAP_TASK_NUM] = String
				.valueOf(mapTaskNum);
		array[BaseValues.DB_COLUMN_HIVE_SQL_REDUCE_TASK_NUM] = String
				.valueOf(reduceTaskNum);

		tool.addDataToMap(jobId, array);
	}

	private int parseTaskNum(String flag, String jobStr) {
		int taskNum;
		int startPos;
		int endPos;

		String tmpStr;

		taskNum = 0;
		tmpStr = jobStr;
		startPos = tmpStr.indexOf(flag);
		
		if(startPos == -1){
			return 0;
		}
		
		tmpStr = tmpStr.substring(startPos + flag.length());
		endPos = tmpStr.indexOf("}");
		tmpStr = tmpStr.substring(0, endPos);
		taskNum = Integer.parseInt(tmpStr.split(":")[1]);

		return taskNum;
	}
}
