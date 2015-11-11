package org.apache.hadoop.mapreduce.v2.hs.tool;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Main {
	public static void main(String[] args) {
		String jobHistoryPath;
		String flagFilePath;
		String curDate;
		String curDay;
		String curTime;
		String[] array;
		int needCleanFalgFile;
		int elapsedTime;
		int threadNum;
		double slowRatio;
		
		HSTool hst;
		
		needCleanFalgFile = 0;
		jobHistoryPath = "/tmp/hadoop-yarn/staging/history/done";
		flagFilePath = "/home/data/programs/hadoop/hs-parse-flag.txt";
		elapsedTime = BaseValues.DEFAULT_ELAPSED_TIME;
		slowRatio = BaseValues.DEFAULT_SLOW_RATIO;
		threadNum = BaseValues.DEFAULT_THREAD_NUM;
		
		curDate = getCurDayTime();
		curDay = curDate.split(" ")[0];
		curTime = curDate.split(" ")[1];
		if(curTime.startsWith("00:2")){
		  needCleanFalgFile = 1;
		}
		
		System.out.println(curDate + ":" + curDay + ":" + curTime);
		
		if (args.length > 0) {
			for(String param: args){
			  array = param.split("=");
			  
			  if(array[0].equals(BaseValues.PARAM_NAME_ELAPSED_TIME)){
			    elapsedTime = Integer.parseInt(array[1]);
			  }else if (array[0].equals(BaseValues.PARAM_NAME_SLOW_RATIO)){
			    slowRatio = Double.parseDouble(array[1]);
			  }else if (array[0].equals(BaseValues.PARAM_NAME_JOBHISTORY_DES_DIR)){
			    jobHistoryPath = array[1];
			  }else if (array[0].equals(BaseValues.PARAM_NAME_THREAD_NUM)){
          threadNum = Integer.parseInt(array[1]);
        }else if (array[0].equals(BaseValues.PARAM_NAME_FLAG_FILE_PATH)){
          flagFilePath = array[1];
        }else if (array[0].equals(BaseValues.PARAM_NAME_TIME_STAMP_DIR)){
          jobHistoryPath = array[1] + curDay;
        }
			}
		}
		
    System.out
        .println(String
            .format(
                "jobhistory path is %s, elapsedTime is %s, slowRatio is %s, threadNum is %s, flagFilePath is %s, needCleanFlagFile is %s",
                jobHistoryPath, elapsedTime, slowRatio, threadNum,
                flagFilePath, needCleanFalgFile));
    hst = new HSTool(elapsedTime, slowRatio, threadNum, jobHistoryPath, flagFilePath, needCleanFalgFile);
    hst.getHistoryData();
	}
	
	private static String getCurDayTime() {
    Calendar calendar = Calendar.getInstance();
    Date date = calendar.getTime();
    DateFormat df = new SimpleDateFormat("/yyyy/MM/dd HH:mm:ss");

    return df.format(date);
  }
}
