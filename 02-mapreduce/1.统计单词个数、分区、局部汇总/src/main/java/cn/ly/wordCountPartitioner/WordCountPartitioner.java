package cn.ly.wordCountPartitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartitioner extends Partitioner<Text, IntWritable> {
    public int getPartition(Text text, IntWritable intWritable, int i) {
        String firLetter = text.toString().substring(0, 1);
        char[] charArray = firLetter.toCharArray();
        int result = charArray[0];
        return result % 2 == 0 ? 0 : 1;
    }
}
