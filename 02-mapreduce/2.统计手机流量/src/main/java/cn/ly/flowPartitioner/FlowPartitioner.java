package cn.ly.flowPartitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * K2 V2 对应的是map输出kv类型
 */
public class FlowPartitioner extends Partitioner<Text, FlowBean> {

    public static HashMap<String, Integer> provinceDict = new HashMap();

    static {
        provinceDict.put("136", 0);
        provinceDict.put("137", 1);
        provinceDict.put("138", 2);
        provinceDict.put("139", 3);
    }

    public int getPartition(Text text, FlowBean flowBean, int i) {
        String prefix = text.toString().substring(0, 3);
        Integer provinceId = provinceDict.get(prefix);
        return provinceId == null ? 4 : provinceId;
    }
}
