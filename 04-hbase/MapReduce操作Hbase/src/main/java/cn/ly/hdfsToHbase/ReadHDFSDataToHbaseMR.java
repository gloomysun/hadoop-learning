package cn.ly.hdfsToHbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ReadHDFSDataToHbaseMR extends Configured implements Tool {
    public int run(String[] strings) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("fs.defaultFS", "hdfs://192.168.74.129:9000");
        conf.set("hbase.zookeeper.quorum", "vm1");
        System.setProperty("HADOOP_USER_NAME", "root");
        FileSystem fs = FileSystem.get(conf);

        Job job = Job.getInstance(conf);

        job.setJarByClass(ReadHDFSDataToHbaseMR.class);
        job.setMapperClass(HDFSToHbaseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        TableMapReduceUtil.initTableReducerJob("student", HDFSToHbaseReducer.class, job,
                null, null, null, null, false);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);

        job.setOutputFormatClass(TableOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("/input"));

        boolean isDone = job.waitForCompletion(true);
        return isDone ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new ReadHDFSDataToHbaseMR(),args);
        System.exit(run);

    }
}
