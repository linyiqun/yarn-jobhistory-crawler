package org.apache.hadoop.mapreduce.v2.hs.tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobACLsManager;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.hs.tool.CompletedJob;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobIndexInfo;

import com.google.common.annotations.VisibleForTesting;

public class HistoryFileInfo {
	private Path historyFile;
	private Path confFile;
	private Path summaryFile;
	private JobIndexInfo jobIndexInfo;
	private HistoryInfoState state;

	private static enum HistoryInfoState {
		IN_INTERMEDIATE, IN_DONE, DELETED, MOVE_FAILED
	};

	public HistoryFileInfo(Path historyFile, Path confFile, Path summaryFile,
			JobIndexInfo jobIndexInfo, boolean isInDone) {
		this.historyFile = historyFile;
		this.confFile = confFile;
		this.summaryFile = summaryFile;
		this.jobIndexInfo = jobIndexInfo;
		state = isInDone ? HistoryInfoState.IN_DONE
				: HistoryInfoState.IN_INTERMEDIATE;
	}

	@Override
	public String toString() {
		return "HistoryFileInfo jobID " + getJobId() + " historyFile = "
				+ historyFile;
	}

	/**
	 * Parse a job from the JobHistoryFile, if the underlying file is not going
	 * to be deleted.
	 * 
	 * @return the Job or null if the underlying file was deleted.
	 * @throws IOException
	 *             if there is an error trying to read the file.
	 */
	public synchronized Job loadJob(boolean loadTask) throws IOException {
		return new CompletedJob(new Configuration(), jobIndexInfo.getJobId(),
				historyFile, loadTask, jobIndexInfo.getUser(), this,
				new JobACLsManager(new Configuration()));
	}

	/**
	 * Return the history file. This should only be used for testing.
	 * 
	 * @return the history file.
	 */
	synchronized Path getHistoryFile() {
		return historyFile;
	}

	public JobIndexInfo getJobIndexInfo() {
		return jobIndexInfo;
	}

	public JobId getJobId() {
		return jobIndexInfo.getJobId();
	}

	public synchronized Path getConfFile() {
		return confFile;
	}

}
