package org.apache.hadoop.mapreduce.v2.hs.tool;

import java.util.Comparator;

import org.apache.hadoop.fs.FileStatus;

public class FileStatusComparator implements Comparator<FileStatus> {
  public int compare(FileStatus fs1, FileStatus fs2) {
    Long lastModifyTime1 = fs1.getModificationTime();
    Long lastModifyTime2 = fs2.getModificationTime();
    
    return lastModifyTime1.compareTo(lastModifyTime2);
  }
}
