package cn.ly.flowSort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    /*************************************************************************************************
     1363157993055 	1116		954		200
     手机号码			上行流量    下行流量  总流量
     *************************************************************************************************/
    FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");

        String phone = fields[0];
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);

        flowBean.setFlow(upFlow, downFlow);

        context.write(flowBean, new Text(phone));

    }
}
