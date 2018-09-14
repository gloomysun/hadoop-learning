package cn.ly.orderSort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderSortPartitioner extends Partitioner<OrderBean,NullWritable> {
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int i) {
        return (orderBean.getOrderId().hashCode() & 2147483647) % i;
    }
}
