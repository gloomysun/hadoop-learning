package cn.ly.wholeFileInputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
    private Text filenameKey;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取切片信息
        InputSplit split = context.getInputSplit();
        //获取切片路径
        Path path = ((FileSplit) split).getPath();
        //根据切片路径获取文件名称
        filenameKey = new Text(path.toString());
    }

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        // 文件名称为key
        context.write(filenameKey, value);

    }
}
