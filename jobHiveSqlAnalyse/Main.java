package org.apache.hadoop.mapreduce.v2.hs.tool.sqlanalyse;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Main {
	public static void main(String[] args){
		HiveSqlAnalyseTool tool;
		String historyJobPath;
		String dirType;
		String dirPrefix;
		
		historyJobPath = "";
		getPreviousDayTime();
		if(args.length == 2){
			dirType = args[0];
			
			if(dirType.equals("dateTimeDir")){
				dirPrefix = args[1];
				historyJobPath = dirPrefix + getPreviousDayTime();
			}else if (dirType.equals("desDir")){
				historyJobPath = args[1]; 
			}
			
		}else {
			System.out.println("input prarm is incorrect");
			return;
		}
		
		System.out.println("jobHistory dir path is " + historyJobPath);
		tool = new HiveSqlAnalyseTool(historyJobPath);
		tool.readJobInfoFiles();
	}
	
	private static String  getPreviousDayTime(){
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DATE, -1); //得到前一天
		Date date = calendar.getTime();
		DateFormat df = new SimpleDateFormat("/yyyy/MM/dd");
		
		return df.format(date);
	}
}
