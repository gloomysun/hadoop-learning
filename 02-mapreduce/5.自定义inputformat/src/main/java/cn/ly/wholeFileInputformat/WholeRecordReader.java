package cn.ly.wholeFileInputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeRecordReader extends RecordReader<NullWritable, BytesWritable> {
    private FileSplit split;
    private Configuration configuration;
    private BytesWritable value = new BytesWritable();
    private boolean processed = false;

    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //获取传递过来的数据
        this.split = (FileSplit) inputSplit;
        this.configuration = taskAttemptContext.getConfiguration();
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!processed) {
            //1.定义缓存
            byte[] contents = new byte[(int) split.getLength()];
            //2.获取文件系统
            Path path = split.getPath();
            FileSystem fs = path.getFileSystem(configuration);
            //3.读取内容
            FSDataInputStream fis = null;
            try {
                //3.1打开输入流
                fis = fs.open(path);
                //3.2读取文件内容
                IOUtils.readFully(fis, contents, 0, contents.length);
                //3.3输出文件内容
                value.set(contents, 0, contents.length);
            } catch (Exception e) {

            } finally {
                IOUtils.closeStream(fis);
            }
            processed = true;
            return true;
        }
        return false;
    }

    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return processed?1:0;
    }

    public void close() throws IOException {

    }
}
