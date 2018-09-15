### 小文件处理（自定义InputFormat）
1）需求  
无论hdfs还是mapreduce，对于小文件都有损效率，实践中，
又难免面临处理大量小文件的场景，此时，就需要有相应解决方案。
将多个小文件合并成一个文件SequenceFile，SequenceFile里面
存储着多个文件，存储的形式为文件路径+名称为key，文件内容为value。


2）分析  
小文件的优化无非以下几种方式：
（1）在数据采集的时候，就将小文件或小批数据合成大文件再上传HDFS  
（2）在业务处理之前，在HDFS上使用mapreduce程序对小文件进行合并  
（3）在mapreduce处理时，可采用CombineTextInputFormat提高效率  

3）具体实现  
采用自定义InputFormat的方式，处理输入小文件的问题。 
（1）自定义一个InputFormat  
（2）改写RecordReader，实现一次读取一个完整文件封装为KV  
（3）在输出时使用SequenceFileOutPutFormat输出合并文件  

