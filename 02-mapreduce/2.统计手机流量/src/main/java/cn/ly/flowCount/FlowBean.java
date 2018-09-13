package cn.ly.flowCount;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 封装流量的bean
 */
public class FlowBean implements Writable {
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    //反序列化时要调用无参构造
    public FlowBean() {
        super();
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setFlow(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    //序列化
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    //反序列化
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();
    }


}
