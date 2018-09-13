package cn.ly.wordCountCombiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.将maptask传递的文本转成string
        String line = value.toString();
        //2.根据空格切分
        String[] words = line.split(" ");
        //3.将单词输出为<单词,1>
        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
