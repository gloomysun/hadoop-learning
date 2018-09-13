package cn.ly.wordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 统计一堆文件中单词出现的个数（WordCount案例）
 */
public class WordCountDriver {

    public static void main(String[] args) throws Exception {
        //1.获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2.指定jar包所在的本地路径
        job.setJarByClass(WordCountDriver.class);
        //3.指定使用的map 和reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //4.指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5.指定reducer输出数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //6.指定输入原始文件目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //7.提交job到yarn
        boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);
    }

}
