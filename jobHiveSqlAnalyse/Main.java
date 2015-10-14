package org.apache.hadoop.mapreduce.v2.hs.tool.sqlanalyse;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Main {
	public static void main(String[] args) {
		int threadNum;
		int writedb;
		String historyJobPath;
		String dirType;
		String dirPrefix;
		String[] params;

		HiveSqlAnalyseTool tool;

		threadNum = 5;
		writedb = 0;
		historyJobPath = "";
		if (args.length >= 2) {
			dirType = args[0];

			if (dirType.equals("dateTimeDir")) {
				dirPrefix = args[1];
				historyJobPath = dirPrefix + getPreviousDayTime();
				writedb = 1;
			} else if (dirType.equals("desDir")) {
				historyJobPath = args[1];
				writedb = 0;
			}
			
			if(args.length > 2){
			  for(int i=2; i<args.length; i++){
			    params = args[i].split("=");
			    
			    if(params[0].equals("-threadnum")){
			      threadNum = Integer.parseInt(params[1]);
			    }else if(params[0].equals("-writedb")){
			      writedb = Integer.parseInt(params[1]);
			    }
			  }
			}
		} else {
			System.out.println("input prarm is incorrect");
			return;
		}

		System.out.println("dirType is " + dirType + ", jobHistory dir path is "
				+ historyJobPath + ", parse thread num is " + threadNum + ", writedb is " + writedb);
		tool = new HiveSqlAnalyseTool(dirType, historyJobPath, threadNum, writedb);
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
