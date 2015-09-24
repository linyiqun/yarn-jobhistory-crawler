package org.apache.hadoop.mapreduce.v2.hs.tool;

public class Main {
	public static void main(String[] args) {
		String jobHistoryPath;
		HSTool hst;

		jobHistoryPath = "/tmp/hadoop-yarn/staging/history/done";
		if (args.length > 0) {
			jobHistoryPath = args[0];
			System.out.println("jobhistory state store path is "
					+ jobHistoryPath);
		}

		hst = new HSTool(jobHistoryPath);
		hst.getHistoryData();
	}
}
