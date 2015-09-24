package org.apache.hadoop.mapreduce.v2.hs.tool;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.hs.tool.HistoryFileInfo;
import org.apache.hadoop.mapreduce.v2.jobhistory.FileNameIndexUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobIndexInfo;

import com.google.common.annotations.VisibleForTesting;

public class HSTool {
	private static String DONE_BEFORE_SERIAL_TAIL = JobHistoryUtils
			.doneSubdirsBeforeSerialTail();

	String jobHistoryPath;
	Path doneDirPrefixPath;
	FileContext doneDirFc;

	ArrayList<HistoryFileInfo> historyFileInfos;

	public HSTool(String jobHistoryPath) {
		this.jobHistoryPath = jobHistoryPath;

		this.historyFileInfos = new ArrayList<HistoryFileInfo>();
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

		fileStatus = null;
		try {
			fileStatus = findTimestampedDirectories();
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
				try {
					addDirectoryToJobListCache(fs.getPath());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		System.out.println("history fileInfo size is "
				+ this.historyFileInfos.size());
		for (HistoryFileInfo hfi : this.historyFileInfos) {
			System.out.println("file jobId is " + hfi.getJobId());

			parseCompleteJob(hfi, true);
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

	private void parseCompleteJob(HistoryFileInfo hfi, boolean loadTask) {
		Job job;
		Task task;
		Map<TaskId, Task> taskInfos;

		job = null;
		try {
			job = hfi.loadJob(loadTask);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("job info : job user is" + job.getUserName()
				+ ", map num is " + job.getTotalMaps() + ", job name is "
				+ job.getName() + ", start time is "
				+ job.getReport().getStartTime() + ", finish time is "
				+ job.getReport().getFinishTime());

		taskInfos = job.getTasks();
		System.out.println("job task total num is " + taskInfos.size());

		for (Map.Entry<TaskId, Task> entry : taskInfos.entrySet()) {
			task = entry.getValue();
			System.out.println("task id is " + task.getID()
					+ "task start time is " + task.getReport().getStartTime());
		}

	}
}
