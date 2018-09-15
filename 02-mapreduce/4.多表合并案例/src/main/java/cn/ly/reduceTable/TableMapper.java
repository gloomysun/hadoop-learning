package cn.ly.reduceTable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    TableBean tableBean = new TableBean();
    Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取输入文件类型
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String filename = fileSplit.getPath().getName();
        //获取读取内容
        String line = value.toString();
        String fields[] = line.split("\t");
        //对不同文件不同操作
        if (filename.startsWith("order")) {
            tableBean.setOrderId(fields[0]);
            tableBean.setPid(fields[1]);
            tableBean.setAmount(Integer.parseInt(fields[2]));
            tableBean.setFlag("0");
            tableBean.setPname("");

            text.set(fields[1]);
        } else {
            tableBean.setPid(fields[0]);
            tableBean.setPname(fields[1]);
            tableBean.setFlag("1");
            tableBean.setOrderId("");
            tableBean.setAmount(0);

            text.set(fields[0]);
        }

        context.write(text, tableBean);
    }
}
