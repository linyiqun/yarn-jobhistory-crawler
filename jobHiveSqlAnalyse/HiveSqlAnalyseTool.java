package org.apache.hadoop.mapreduce.v2.hs.tool.sqlanalyse;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.IOUtils;

public class HiveSqlAnalyseTool {
	private int threadNum;
	private int writedb;
	private String dirType;
	private String jobHistoryPath;

	private FileContext doneDirFc;
	private Path doneDirPrefixPath;

	private LinkedList<FileStatus> fileStatusList;
	private HashMap<String, String[]> dataInfos;
	private DbClient dbClient;

	public HiveSqlAnalyseTool(String dirType, String jobHistoryPath,
			int threadNum, int writedb) {
		this.threadNum = threadNum;
		this.writedb = writedb;
		this.dirType = dirType;
		this.jobHistoryPath = jobHistoryPath;

		this.dataInfos = new HashMap<String, String[]>();
		this.fileStatusList = new LinkedList<FileStatus>();
		this.dbClient = new DbClient(BaseValues.DB_URL,
				BaseValues.DB_USER_NAME, BaseValues.DB_PASSWORD,
				BaseValues.DB_HIVE_SQL_STAT_TABLE_NAME);

		try {
			doneDirPrefixPath = FileContext.getFileContext(new Configuration())
					.makeQualified(new Path(this.jobHistoryPath));
			doneDirFc = FileContext.getFileContext(doneDirPrefixPath.toUri());
		} catch (UnsupportedFileSystemException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void readJobInfoFiles() {
		List<FileStatus> files;

		files = new ArrayList<FileStatus>();
		try {
			files = scanDirectory(doneDirPrefixPath, doneDirFc, files);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (files != null) {
			for (FileStatus fs : files) {
				// parseFileInfo(fs);
			}
			System.out.println("files num is " + files.size());
			System.out
					.println("fileStatusList size is" + fileStatusList.size());

			ParseThread[] threads;
			threads = new ParseThread[threadNum];
			for (int i = 0; i < threadNum; i++) {
				System.out.println("thread " + i + "start run");
				threads[i] = new ParseThread(this, fileStatusList, dataInfos);
				threads[i].start();
			}

			for (int i = 0; i < threadNum; i++) {
				System.out.println("thread " + i + "join run");
				try {
					if (threads[i] != null) {
						threads[i].join();
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} else {
			System.out.println("files is null");
		}

		printStatDatas();
	}

	protected List<FileStatus> scanDirectory(Path path, FileContext fc,
			List<FileStatus> jhStatusList) throws IOException {
		path = fc.makeQualified(path);
		System.out.println("dir path is " + path.getName());
		try {
			RemoteIterator<FileStatus> fileStatusIter = fc.listStatus(path);
			while (fileStatusIter.hasNext()) {
				FileStatus fileStatus = fileStatusIter.next();
				Path filePath = fileStatus.getPath();

				if (fileStatus.isFile()) {
					jhStatusList.add(fileStatus);
					fileStatusList.add(fileStatus);
				} else if (fileStatus.isDirectory()) {
					scanDirectory(filePath, fc, jhStatusList);
				}
			}
		} catch (FileNotFoundException fe) {
			System.out.println("Error while scanning directory " + path);
		}

		return jhStatusList;
	}

	private void parseFileInfo(FileStatus fs) {
		String resultStr;
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

		resultStr = "";
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
		} else {
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

			System.out.println("jobId is " + jobId);
			System.out.println("jobName is " + jobName);
			System.out.println("username is " + username);
			System.out.println("map task num is " + mapTaskNum);
			System.out.println("reduce task num is " + reduceTaskNum);
			System.out.println("launchTime is " + launchTime);
			System.out.println("finishTime is " + finishTime);
			System.out.println("hive query sql is " + hiveSql);
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

		dataInfos.put(jobId, array);
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

		dataInfos.put(jobId, array);
	}

	private int parseTaskNum(String flag, String jobStr) {
		int taskNum;
		int startPos;
		int endPos;

		String tmpStr;

		taskNum = 0;
		tmpStr = jobStr;
		startPos = tmpStr.indexOf(flag);

		if (startPos == -1) {
			return 0;
		}

		tmpStr = tmpStr.substring(startPos + flag.length());
		endPos = tmpStr.indexOf("}");
		tmpStr = tmpStr.substring(0, endPos);
		taskNum = Integer.parseInt(tmpStr.split(":")[1]);

		return taskNum;
	}

	private void printStatDatas() {
		String jobId;
		String jobInfo;
		String[] infos;

		if (dbClient != null) {
			dbClient.createConnection();
		}

		if (dataInfos != null) {
			System.out.println("map data size is" + dataInfos.size() + ", writedb is" + writedb);
			
			if (dbClient != null && writedb == 1) {
				dbClient.insertDataBatch(dataInfos);
			}
		}

		/*for (Entry<String, String[]> entry : this.dataInfos.entrySet()) {
			jobId = entry.getKey();
			infos = entry.getValue();

			jobInfo = String
					.format("jobId is %s, jobName:%s, usrname:%s, launchTime:%s, finishTime:%s, mapTaskNum:%s, reduceTaskNum:%s, querySql:%s",
							jobId, infos[1], infos[2], infos[3], infos[4],
							infos[5], infos[6], infos[7]);
			// System.out.println("job detail info " + jobInfo);

			if (dbClient != null && dirType.equals("dateTimeDir")) {
				dbClient.insertHiveSqlStatData(infos);
			}
		}*/

		if (dbClient != null) {
			dbClient.closeConnection();
		}
	}

	public synchronized FileStatus getOneFile() {
		FileStatus fs;

		fs = null;
		if (fileStatusList != null & fileStatusList.size() > 0) {
			fs = fileStatusList.poll();
		}

		return fs;
	}

	public synchronized void addDataToMap(String jobId, String[] values) {
		if (dataInfos != null) {
			dataInfos.put(jobId, values);
		}
	}
}
