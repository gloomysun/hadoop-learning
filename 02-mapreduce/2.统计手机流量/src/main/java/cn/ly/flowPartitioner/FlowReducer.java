package cn.ly.flowPartitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    FlowBean flowBean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long upFlow = 0;
        long downFlow = 0;
        for (FlowBean flowBean : values) {
            upFlow += flowBean.getUpFlow();
            downFlow += flowBean.getDownFlow();
        }
        flowBean.setFlow(upFlow, downFlow);
        context.write(key, flowBean);
    }
}
