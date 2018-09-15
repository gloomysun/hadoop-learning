package cn.ly.reduceTable;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {


    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        ArrayList<TableBean> beans = new ArrayList<TableBean>();
        TableBean tableBean = new TableBean();
        for (TableBean bean : values) {
            if ("0".equals(bean.getFlag())) {
                TableBean orderBean = new TableBean();
                try {
                    BeanUtils.copyProperties(orderBean, bean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                beans.add(orderBean);
            } else {
                try {
                    BeanUtils.copyProperties(tableBean, bean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        for (TableBean bean : beans) {
            bean.setPname(tableBean.getPname());
            context.write(bean, NullWritable.get());
        }

    }
}
