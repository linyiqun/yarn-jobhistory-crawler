# yarn-jobhistory-crawler
JobHistory上的job信息爬取工具

## 使用说明
1.将程序导出jar包,比如取名叫hs-parse.jar

2.写一个脚本hs-parse.sh, 指定classpath运行
```
/home/data/programs/jdk/jdk-current/bin/java -classpath /home/data/programs/hadoop/hadoop-current/etc/hadoop:
/home/data/programs/hadoop/hadoop-current/etc/hadoop:/home/data/programs/hadoop/hadoop-current/share/hadoop/common/lib/*:
/home/data/programs/hadoop/hadoop-current/share/hadoop/common/*:/home/data/programs/hadoop/hadoop-current/share/hadoop/hdfs:
/home/data/programs/hadoop/hadoop-current/share/hadoop/hdfs/lib/*:/home/data/programs/hadoop/hadoop-current/share/hadoop/hdfs/*:
/home/data/programs/hadoop/hadoop-current/share/hadoop/yarn/lib/*:/home/data/programs/hadoop/hadoop-current/share/hadoop/yarn/*:
/home/data/programs/hadoop/hadoop-current/share/hadoop/mapreduce/lib/*:
/home/data/programs/hadoop/hadoop-current/share/hadoop/mapreduce/*:
/home/data/programs/hadoop/hadoop-current/contrib/capacity-scheduler/*.jar:
/home/data/programs/hadoop-current/share/hadoop/hdfs/lib/*:
/home/data/programs/hadoop/hs-parse.jar -Dproc_balancer -Xmx1024m -XX:ParallelGCThreads=5 
-Djava.net.preferIPv4Stack=true -Dhadoop.log.dir=/home/data/programs/hadoop/hadoop-current/logs 
-Dhadoop.log.file=hadoop.log -Dhadoop.home.dir=/home/data/programs/hadoop/hadoop-current -Dhadoop.id.str=data 
-Dhadoop.root.logger=INFO,console -Djava.library.path=/home/data/programs/hadoop/hadoop-current/lib/native 
-Dhadoop.policy.file=hadoop-policy.xml -Djava.net.preferIPv4Stack=true-Dhadoop.security.logger=INFO,NullAppender 
org.apache.hadoop.mapreduce.v2.hs.tool.Main $*
```
注意在classpath的最结尾需要加上导出的jar包的路径,否则找不到执行类名.

3.启动脚本执行
```
source hs-parse.sh /your-path/history/done
```
