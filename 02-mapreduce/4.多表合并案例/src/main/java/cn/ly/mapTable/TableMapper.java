package cn.ly.mapTable;

import cn.ly.reduceTable.TableBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class TableMapper extends Mapper<LongWritable, Text, Text,NullWritable> {

    Map<String, String> pMap = new HashMap<String, String>();
    Text k = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取缓存文件
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("E:\\hadoop\\pd.txt")));

        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            String[] fileds = line.split("\t");
            //缓存到集合
            pMap.put(fileds[0], fileds[1]);
        }
        // 4 关流
        reader.close();

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1 获取一行
        String line = value.toString();

        // 2 截取
        String[] fields = line.split("\t");

        // 3 获取订单id
        String orderId = fields[1];

        // 4 获取商品名称
        String pdName = pMap.get(orderId);

        // 5 拼接
        k.set(line + "\t"+ pdName);

        // 6 写出
        context.write(k, NullWritable.get());


    }
}
