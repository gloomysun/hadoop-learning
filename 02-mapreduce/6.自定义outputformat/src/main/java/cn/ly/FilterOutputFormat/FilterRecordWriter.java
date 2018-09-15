package cn.ly.FilterOutputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream myOut = null;
    FSDataOutputStream otherOut = null;

    public FilterRecordWriter(TaskAttemptContext job) {
        //1.获取文件系统
        FileSystem fs;

        try {
            fs = FileSystem.get(job.getConfiguration());

            //2.创建文件输出路径
            Path myPath = new Path("e:/my.log");
            Path otherPath = new Path("e:/other.log");
            //3.创建输出流
            myOut = fs.create(myPath);
            otherOut = fs.create(otherPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        //判断是否包含github输出到不同文件
        if (text.toString().contains("github")) {
            myOut.write(text.toString().getBytes());
        } else {
            otherOut.write(text.toString().getBytes());
        }
    }

    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(myOut);
        IOUtils.closeStream(otherOut);
    }
}
