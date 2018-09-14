package cn.ly.orderSort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private String orderId;
    private Double price;

    public OrderBean() {
        super();
    }

    public void setOrder(String orderId, Double price) {
        this.orderId = orderId;
        this.price = price;
    }

    public String getOrderId() {
        return orderId;
    }

    public Double getPrice() {
        return price;
    }


    public int compareTo(OrderBean o) {
        //先按订单从小到大排序
        int result = this.orderId.compareTo(o.getOrderId());
        if (result == 0) {
            //再按价格从高到低排序
            result = this.price > o.getPrice() ? -1 : 1;
        }
        return result;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeDouble(price);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.price = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return this.getOrderId() + "\t" + this.getPrice();
    }
}
