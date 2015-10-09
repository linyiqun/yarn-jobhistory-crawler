package org.apache.hadoop.mapreduce.v2.hs.tool.sqlanalyse;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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

public class HiveSqlAnalyseTool {
	private String jobHistoryPath;

	private FileContext doneDirFc;
	private Path doneDirPrefixPath;

	private HashMap<String, String[]> dataInfos;
	private DbClient dbClient;

	public HiveSqlAnalyseTool(String jobHistoryPath) {
		this.jobHistoryPath = jobHistoryPath;

		this.dataInfos = new HashMap<String, String[]>();
		// this.dbClient = new DbClient(BaseValues.DB_URL,
		// BaseValues.DB_USER_NAME, BaseValues.DB_PASSWORD,
		// BaseValues.DB_HIVE_SQL_STAT_TABLE_NAME);

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
				System.out.println("Path is " + fs.getPath().getName());
				parseFileInfo(fs);
			}
			System.out.println("files num is " + files.size());
		} else {
			System.out.println("files is null");
		}

		printStatDatas();
	}

	protected static List<FileStatus> scanDirectory(Path path, FileContext fc,
			List<FileStatus> jhStatusList) throws IOException {
		path = fc.makeQualified(path);
		System.out.println("dir path is " + path.getName());
		try {
			RemoteIterator<FileStatus> fileStatusIter = fc.listStatus(path);
			while (fileStatusIter.hasNext()) {
				FileStatus fileStatus = fileStatusIter.next();
				Path filePath = fileStatus.getPath();
				System.out.println("child file path is " + filePath.getName());

				if (fileStatus.isFile()) {
					jhStatusList.add(fileStatus);
				} else if (fileStatus.isDirectory()) {
					scanDirectory(filePath, fc, jhStatusList);
				}
			}
		} catch (FileNotFoundException fe) {
			System.out.println("Error while scanning directory " + path);
		}
		return jhStatusList;
	}

	private String parseFileInfo(FileStatus fs) {
		String resultStr;
		String str;
		String username;
		String fileType;
		String jobId;
		String hiveSql;

		int startPos;
		int endPos;
		long launchTime;
		long finishTime;
		String usernameFlag;
		String launchTimeFlag;
		String finishTimeFlag;

		Path path;
		FileSystem fileSystem;
		FSDataInputStream in;

		resultStr = "";
		fileType = "";
		hiveSql = "";
		jobId = "";
		username = "";
		launchTime = 0;
		finishTime = 0;
		usernameFlag = "<value>";
		launchTimeFlag = "\"launchTime\":";
		finishTimeFlag = "\"finishTime\":";

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
		}

		try {
			fileSystem = path.getFileSystem(new Configuration());
			in = fileSystem.open(path);

			while ((str = in.readLine()) != null) {
				if (str.contains("mapreduce.job.user.name")) {
					System.out.println(str);
					startPos = str.indexOf(usernameFlag);
					endPos = str.indexOf("</value>");
					username = str.substring(startPos + usernameFlag.length(),
							endPos);
				} else if (str.contains("hive.query.string")) {
					System.out.println(str);
					startPos = str.indexOf(usernameFlag);
					endPos = str.indexOf("</value>");
					hiveSql = str.substring(startPos + usernameFlag.length(),
							endPos);
				} else if (str.startsWith("{\"type\":\"JOB_INITED\"")) {
					System.out.println(str);
					startPos = str.indexOf(launchTimeFlag);
					str = str.substring(startPos + launchTimeFlag.length());
					System.out.println("launch time is " + str);
					endPos = str.indexOf(",");
					launchTime = Long.parseLong(str.substring(0, endPos));
				} else if (str.startsWith("{\"type\":\"JOB_FINISHED\"")) {
					System.out.println(str);
					startPos = str.indexOf(finishTimeFlag);
					str = str.substring(startPos + finishTimeFlag.length());
					endPos = str.indexOf(",");
					System.out.println("finish time is " + str);
					finishTime = Long.parseLong(str.substring(0, endPos));
				}
			}

			System.out.println("jobId is " + jobId);
			System.out.println("username is " + username);
			System.out.println("launchTime is " + launchTime);
			System.out.println("finishTime is " + finishTime);
			System.out.println("hive query sql is " + hiveSql);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (fileType.equals("config")) {
			insertConfParseData(jobId, username, hiveSql);
		} else if (fileType.equals("info")) {
			insertJobInfoParseData(jobId, launchTime, finishTime);
		}

		return resultStr;
	}

	private void insertConfParseData(String jobId, String username, String sql) {
		String[] array;

		if (dataInfos.containsKey(jobId)) {
			array = dataInfos.get(jobId);
		} else {
			array = new String[BaseValues.DB_COLUMN_HIVE_SQL_LEN];
		}

		array[BaseValues.DB_COLUMN_HIVE_SQL_JOBID] = jobId;
		array[BaseValues.DB_COLUMN_HIVE_SQL_USERNAME] = username;
		array[BaseValues.DB_COLUMN_HIVE_SQL_HIVE_SQL] = sql;

		dataInfos.put(jobId, array);
	}

	private void insertJobInfoParseData(String jobId, long launchTime,
			long finishedTime) {
		String[] array;

		if (dataInfos.containsKey(jobId)) {
			array = dataInfos.get(jobId);
		} else {
			array = new String[4];
		}

		array[BaseValues.DB_COLUMN_HIVE_SQL_JOBID] = jobId;
		array[BaseValues.DB_COLUMN_HIVE_SQL_START_TIME] = String
				.valueOf(launchTime);
		array[BaseValues.DB_COLUMN_HIVE_SQL_FINISH_TIME] = String
				.valueOf(finishedTime);

		dataInfos.put(jobId, array);
	}

	private void printStatDatas() {
		String jobId;
		String jobInfo;
		String[] infos;

		if (dbClient != null) {
			dbClient.createConnection();
		}

		for (Entry<String, String[]> entry : this.dataInfos.entrySet()) {
			jobId = entry.getKey();
			infos = entry.getValue();

			jobInfo = String
					.format("jobId is %s, usrname:%s, launchTime:%s, finishTime:%s, querySql:%s",
							jobId, infos[1], infos[2], infos[3], infos[4]);
			System.out.println("job detail info " + jobInfo);

			if (dbClient != null) {
				dbClient.insertHiveSqlStatData(infos);
			}
		}

		if (dbClient != null) {
			dbClient.closeConnection();
		}
	}
}
