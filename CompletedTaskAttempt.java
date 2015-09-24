package org.apache.hadoop.mapreduce.v2.hs.tool;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.api.records.Phase;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.util.Records;

public class CompletedTaskAttempt implements TaskAttempt {

	private final TaskAttemptInfo attemptInfo;
	private final TaskAttemptId attemptId;
	private final TaskAttemptState state;
	private final List<String> diagnostics = new ArrayList<String>();
	private TaskAttemptReport report;

	private String localDiagMessage;

	CompletedTaskAttempt(TaskId taskId, TaskAttemptInfo attemptInfo) {
		this.attemptInfo = attemptInfo;
		this.attemptId = TypeConverter.toYarn(attemptInfo.getAttemptId());
		if (attemptInfo.getTaskStatus() != null) {
			this.state = TaskAttemptState.valueOf(attemptInfo.getTaskStatus());
		} else {
			this.state = TaskAttemptState.KILLED;
			localDiagMessage = "Attmpt state missing from History : marked as KILLED";
			diagnostics.add(localDiagMessage);
		}
		if (attemptInfo.getError() != null) {
			diagnostics.add(attemptInfo.getError());
		}
	}

	@Override
	public NodeId getNodeId() throws UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}

	@Override
	public ContainerId getAssignedContainerID() {
		return attemptInfo.getContainerId();
	}

	@Override
	public String getAssignedContainerMgrAddress() {
		return attemptInfo.getHostname() + ":" + attemptInfo.getPort();
	}

	@Override
	public String getNodeHttpAddress() {
		return attemptInfo.getTrackerName() + ":" + attemptInfo.getHttpPort();
	}

	@Override
	public String getNodeRackName() {
		return attemptInfo.getRackname();
	}

	@Override
	public Counters getCounters() {
		return attemptInfo.getCounters();
	}

	@Override
	public TaskAttemptId getID() {
		return attemptId;
	}

	@Override
	public float getProgress() {
		return 1.0f;
	}

	@Override
	public synchronized TaskAttemptReport getReport() {
		if (report == null) {
			constructTaskAttemptReport();
		}
		return report;
	}

	@Override
	public Phase getPhase() {
		return Phase.CLEANUP;
	}

	@Override
	public TaskAttemptState getState() {
		return state;
	}

	@Override
	public boolean isFinished() {
		return true;
	}

	@Override
	public List<String> getDiagnostics() {
		return diagnostics;
	}

	@Override
	public long getLaunchTime() {
		return attemptInfo.getStartTime();
	}

	@Override
	public long getFinishTime() {
		return attemptInfo.getFinishTime();
	}

	@Override
	public long getShuffleFinishTime() {
		return attemptInfo.getShuffleFinishTime();
	}

	@Override
	public long getSortFinishTime() {
		return attemptInfo.getSortFinishTime();
	}

	@Override
	public int getShufflePort() {
		return attemptInfo.getShufflePort();
	}

	private void constructTaskAttemptReport() {
		report = Records.newRecord(TaskAttemptReport.class);

		report.setTaskAttemptId(attemptId);
		report.setTaskAttemptState(state);
		report.setProgress(getProgress());
		report.setStartTime(attemptInfo.getStartTime());
		report.setFinishTime(attemptInfo.getFinishTime());
		report.setShuffleFinishTime(attemptInfo.getShuffleFinishTime());
		report.setSortFinishTime(attemptInfo.getSortFinishTime());
		if (localDiagMessage != null) {
			report.setDiagnosticInfo(attemptInfo.getError() + ", "
					+ localDiagMessage);
		} else {
			report.setDiagnosticInfo(attemptInfo.getError());
		}
		// report.setPhase(attemptInfo.get); //TODO
		report.setStateString(attemptInfo.getState());
		report.setCounters(TypeConverter.toYarn(getCounters()));
		report.setContainerId(attemptInfo.getContainerId());
		if (attemptInfo.getHostname() == null) {
			report.setNodeManagerHost("UNKNOWN");
		} else {
			report.setNodeManagerHost(attemptInfo.getHostname());
			report.setNodeManagerPort(attemptInfo.getPort());
		}
		report.setNodeManagerHttpPort(attemptInfo.getHttpPort());
	}
}
