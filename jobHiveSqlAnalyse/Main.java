package org.apache.hadoop.mapreduce.v2.hs.tool.sqlanalyse;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Main {
	public static void main(String[] args) {
		int threadNum;
		String historyJobPath;
		String dirType;
		String dirPrefix;

		HiveSqlAnalyseTool tool;

		threadNum = 5;
		historyJobPath = "";
		if (args.length == 2 || args.length == 3) {
			dirType = args[0];

			if (dirType.equals("dateTimeDir")) {
				dirPrefix = args[1];
				historyJobPath = dirPrefix + getPreviousDayTime();
			} else if (dirType.equals("desDir")) {
				historyJobPath = args[1];
			}

			if (args.length == 3) {
				threadNum = Integer.parseInt(args[2]);
			}
		} else {
			System.out.println("input prarm is incorrect");
			return;
		}

		System.out.println("dirType is ," + dirType + "jobHistory dir path is "
				+ historyJobPath + ", parse thread num is " + threadNum);
		tool = new HiveSqlAnalyseTool(dirType, historyJobPath, threadNum);
		tool.readJobInfoFiles();
	}

	private static String getPreviousDayTime() {
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DATE, -1); // 得到前一天
		Date date = calendar.getTime();
		DateFormat df = new SimpleDateFormat("/yyyy/MM/dd");

		return df.format(date);
	}
}
