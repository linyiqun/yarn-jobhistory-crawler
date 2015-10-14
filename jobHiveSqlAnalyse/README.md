## Hive Sql Job分析工具

## 目录类型选择
工具通过传入参数的形式执行,参数主要分为2种,1个是目录类型选择,1个对应的目录前缀
* dateTimeDir,按照时间来指定,默认当前的前一天日期,然后带上jobHistory的存放父目录
* desDir,直接值得目标目录,传入目标包含job信息文件的目录名

## 解析线程数
线程数存在默认值,默认5个解析线程,直接填写数字即可

示例
* java -jar package-jar dateTimeDir /user/history/done -threadnum=10 -writedb=1(后面2个参数可选) 或
* java -jar package-jar desDir /user/history/done/2015/10/11 -threadnum=10 -writedb=1(后面2个参数可选)
