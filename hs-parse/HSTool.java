package org.apache.hadoop.mapreduce.v2.hs.tool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.mapreduce.v2.jobhistory.FileNameIndexUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobIndexInfo;

import com.google.common.annotations.VisibleForTesting;

public class HSTool {
	private static String DONE_BEFORE_SERIAL_TAIL = JobHistoryUtils
			.doneSubdirsBeforeSerialTail();

	String jobHistoryPath;
	String flagFilePath;
	String previousDirName;
	String previousJobId;
	String lastDirName;
  String lastJobId;
  int flagDir;
  int flagFile;
  int needCleanFlagFile;
	int elapsedTime;
	int threadNum;
  double slowRatio;
  
  //total task stat data
  int totalJobNum;
  int totalTaskNum;
  long totalTaskElapsedTime;
  long totalTaskGcTime;
  long totalTaskReadBytes;
  long totalTaskWriteBytes;
  
	Path doneDirPrefixPath;
	FileContext doneDirFc;
  //LinkedList<FileStatus> fileStatusList;
  //List<FileStatus> fileStatusList;
  HashMap<String, String[]> jobDatas;
  HashMap<String, String[]> taskDatas;
  HashMap<String, String[]> taskAttemptDatas;
  LinkedList<HistoryFileInfo> historyFileInfos;
  DbClient dbClient;

	public HSTool(int elapsedTime, double slowRatio, int threadNum, String jobHistoryPath, String flagFilePath, int needCleanFlagFile) {
	  this.flagDir = 0;
	  this.flagFile = 0;
	  this.totalJobNum = 0;
	  this.totalTaskNum = 0;
	  this.totalTaskElapsedTime = 0;
	  this.totalTaskGcTime = 0;
	  this.totalTaskReadBytes = 0;
    this.totalTaskWriteBytes = 0;
	  
	  this.elapsedTime = elapsedTime;
	  this.slowRatio = slowRatio;
	  this.threadNum = threadNum;
		this.jobHistoryPath = jobHistoryPath;
		this.flagFilePath = flagFilePath;
		this.needCleanFlagFile = needCleanFlagFile;
		
		//this.fileStatusList = new LinkedList<FileStatus>();
		this.historyFileInfos = new LinkedList<HistoryFileInfo>();
		this.jobDatas = new HashMap<String, String[]>();
		this.taskDatas = new HashMap<String, String[]>();
		this.taskAttemptDatas = new HashMap<String, String[]>();
		
		this.dbClient = new DbClient(BaseValues.DB_URL,
        BaseValues.DB_USER_NAME, BaseValues.DB_PASSWORD,
        BaseValues.DB_JOB_STAT_ALL_TABLE_NAME);
	}

	public void getHistoryData() {
		String doneDirPrefix = jobHistoryPath;
		List<FileStatus> fileStatus;

		try {
			doneDirPrefixPath = FileContext.getFileContext(new Configuration())
					.makeQualified(new Path(doneDirPrefix));

			doneDirFc = FileContext.getFileContext(doneDirPrefixPath.toUri());
			doneDirFc.setUMask(JobHistoryUtils.HISTORY_DONE_DIR_UMASK);
		} catch (UnsupportedFileSystemException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    if(needCleanFlagFile == 1){
      previousDirName = "";
      previousJobId = "";
      
      System.out.println("flag file need be cleaned");
    }else{
      String[] previousFlags;
      previousFlags = readFlagFile();
      
      previousDirName = previousFlags[0];
      previousJobId = previousFlags[1];
    }
    
    System.out.println("previousDir is " + previousDirName
        + ", previousJobId is " + previousJobId);
		
		fileStatus = new ArrayList<FileStatus>();
		try {
			//fileStatus = findTimestampedDirectories();
		  fileStatus = scanDirectory(doneDirPrefixPath, doneDirFc, fileStatus);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (fileStatus == null) {
			System.out.println("fileStatus is null");
		} else {
			System.out.println("dir fileStatus size is " + fileStatus.size());
			for (FileStatus fs : fileStatus) {
				System.out.println("child path name is "
						+ fs.getPath().getName());
				
				if(!previousJobId.equals("")){
				  if(fs.getPath().getName().startsWith(previousJobId)){
	          flagFile = 1;
	          continue;
	        }
	        
	        if(flagFile == 0){
	          continue;
	        }
				}
				
				System.out.println("adding path name is "
            + fs.getPath().getName());
				try {
					addDirectoryToJobListCache(fs.getPath());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

    if (!historyFileInfos.isEmpty()) {
      lastJobId = historyFileInfos.getLast().getJobId().toString();
      System.out.println("history fileInfo size is "
          + this.historyFileInfos.size() + ", last dir name is " + lastDirName
          + ", last job id is " + lastJobId);
    }
    
    writeLastFileFlag(lastDirName, lastJobId);
		
		ParseThread[] threads;
    threads = new ParseThread[threadNum];
    for (int i = 0; i < threadNum; i++) {
      System.out.println("thread " + i + "start run");
      threads[i] = new ParseThread(this, historyFileInfos);
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
		
		if(dbClient != null){
      dbClient.createConnection();
      
      dbClient.insertJobInfoDataBatch(jobDatas);
      dbClient.insertJobTaskDataBatch(taskDatas);
      dbClient.insertJobTaskAttemptDataBatch(taskAttemptDatas);
      
      dbClient.closeConnection();
    } 
	}

	/**
	 * Finds all history directories with a timestamp component by scanning the
	 * filesystem. Used when the JobHistory server is started.
	 * 
	 * @return list of history directories
	 */
	private List<FileStatus> findTimestampedDirectories() throws IOException {
		List<FileStatus> fsList = JobHistoryUtils.localGlobber(doneDirFc,
				doneDirPrefixPath, DONE_BEFORE_SERIAL_TAIL);
		return fsList;
	}

	private void addDirectoryToJobListCache(Path path) throws IOException {
		List<FileStatus> historyFileList = scanDirectoryForHistoryFiles(path,
				doneDirFc);
		for (FileStatus fs : historyFileList) {
			JobIndexInfo jobIndexInfo = FileNameIndexUtils.getIndexInfo(fs
					.getPath().getName());
			String confFileName = JobHistoryUtils
					.getIntermediateConfFileName(jobIndexInfo.getJobId());
			String summaryFileName = JobHistoryUtils
					.getIntermediateSummaryFileName(jobIndexInfo.getJobId());
			HistoryFileInfo fileInfo = new HistoryFileInfo(fs.getPath(),
					new Path(fs.getPath().getParent(), confFileName), new Path(
							fs.getPath().getParent(), summaryFileName),
					jobIndexInfo, true);
			historyFileInfos.add(fileInfo);
		}
	}

	protected List<FileStatus> scanDirectoryForHistoryFiles(Path path,
			FileContext fc) throws IOException {
		return scanDirectory(path, fc, JobHistoryUtils.getHistoryFileFilter());
	}

	@VisibleForTesting
	protected static List<FileStatus> scanDirectory(Path path, FileContext fc,
			PathFilter pathFilter) throws IOException {
		path = fc.makeQualified(path);
		List<FileStatus> jhStatusList = new ArrayList<FileStatus>();
		try {
			RemoteIterator<FileStatus> fileStatusIter = fc.listStatus(path);
			while (fileStatusIter.hasNext()) {
				FileStatus fileStatus = fileStatusIter.next();
				Path filePath = fileStatus.getPath();
				if (fileStatus.isFile() && pathFilter.accept(filePath)) {
					jhStatusList.add(fileStatus);
				}
			}
		} catch (FileNotFoundException fe) {
			System.out.println("Error while scanning directory " + path);
		}
		return jhStatusList;
	}

	
	
	protected List<FileStatus> scanDirectory(Path path, FileContext fc,
      List<FileStatus> jhStatusList) throws IOException {
    path = fc.makeQualified(path);
    System.out.println("dir path is " + path.getName());
    
    try {
      ArrayList<FileStatus> fsList;
      RemoteIterator<FileStatus> fileStatusIter = fc.listStatus(path);
      fsList = transformFileStatusList(fileStatusIter);
      
      for(FileStatus fileStatus: fsList) {
        Path filePath = fileStatus.getPath();

        if (fileStatus.isFile()) {
          jhStatusList.add(fileStatus);
          //fileStatusList.add(fileStatus);
        } else if (fileStatus.isDirectory()) {
          System.out.println("file dir path is " + filePath.getName());
          if (previousDirName.equals("")
              || filePath.getName().equals(previousDirName)) {
            flagDir = 1;
          }

          if (flagDir == 0) {
            continue;
          }
          
          System.out.println("adding file dir path is " + filePath.getName());
          scanDirectory(filePath, fc, jhStatusList);
          //update the last dir name
          lastDirName = filePath.getName();
        }
      }
    } catch (FileNotFoundException fe) {
      System.out.println("Error while scanning directory " + path);
    }

    return jhStatusList;
  }
  
  public synchronized HistoryFileInfo getOneHistoryFileInfo() {
    HistoryFileInfo hfi;
    
    hfi = null;
    if (historyFileInfos != null & historyFileInfos.size() > 0) {
      hfi = historyFileInfos.poll();
    }

    return hfi;
  }

  public synchronized void addDataToJobInfoMap(String jobId, String[] values) {
    if (jobDatas != null) {
      jobDatas.put(jobId, values);
    }
  }

  public synchronized void addDataToTaskDataMap(String taskId, String[] values) {
    if (taskDatas != null) {
      taskDatas.put(taskId, values);
    }
  }

  public synchronized void addDataToTaskAttemptMap(String taId, String[] values) {
    if (taskAttemptDatas != null) {
      taskAttemptDatas.put(taId, values);
    }
  }
  
  public synchronized void addJobCounter(){
    this.totalJobNum++;
  }
  
  public synchronized void addTaskCounter(int taskNum, long elapsedTime,
      long gcTime, long readBytes, long writeBytes) {
    this.totalTaskNum += taskNum;
    this.totalTaskElapsedTime += (elapsedTime / 1000);
    this.totalTaskGcTime += (gcTime / 1000);
    this.totalTaskReadBytes += readBytes;
    this.totalTaskWriteBytes += writeBytes;
  }
  
  private void writeLastFileFlag(String lastDirName, String lastJobId) {
    StringBuilder strBuilder = new StringBuilder();

    strBuilder.append(lastDirName);
    strBuilder.append("\t");
    strBuilder.append(lastJobId);

    try {
      File file = new File(flagFilePath);
      PrintStream ps = new PrintStream(new FileOutputStream(file));
      ps.print(strBuilder.toString());// 往文件里写入字符串
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  private String[] readFlagFile() {
    String[] array;
    File file;

    array = new String[2];
    file = new File(flagFilePath);
    
    try {
      BufferedReader in = new BufferedReader(new FileReader(file));
      String str;

      while ((str = in.readLine()) != null) {
        array = str.split("\t");
      }
      in.close();
    } catch (IOException e) {
      e.getStackTrace();
    }
    
    if(array[0] == null){
      array[0] = "";
    }
    
    if(array[1] == null){
      array[1] = "";
    }

    return array;
  }
  
  private ArrayList<FileStatus> transformFileStatusList(
      RemoteIterator<FileStatus> fileStatusIter) throws IOException {
    ArrayList<FileStatus> fileList;

    fileList = new ArrayList<FileStatus>();
    while (fileStatusIter.hasNext()) {
      FileStatus fileStatus = fileStatusIter.next();
      fileList.add(fileStatus);
    }

    if(fileList.size() > 0){
      if(fileList.get(0).isDirectory()){
        System.out.println("sorting dir list by update time");
        Collections.sort(fileList, new FileStatusComparator());
      }
    }
    
    return fileList;
  }
}
