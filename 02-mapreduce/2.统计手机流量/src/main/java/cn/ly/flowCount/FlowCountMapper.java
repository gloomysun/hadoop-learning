package cn.ly.flowCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    /*************************************************************************************************
     1363157993055 	13560436666	C4-17-FE-BA-DE-D9:CMCC	120.196.100.99		18	15	1116		954		200
     手机号码										                                    上行流量    下行流量
     *************************************************************************************************/
    FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");

        String phone = fields[1];
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);

        flowBean.setFlow(upFlow, downFlow);

        context.write(new Text(phone), flowBean);


    }
}
