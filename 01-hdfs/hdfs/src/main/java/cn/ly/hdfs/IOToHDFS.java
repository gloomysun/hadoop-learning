package cn.ly.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.*;
import java.net.URI;

/**
 * IO流操作hdfs
 */
public class IOToHDFS {

    //IO流上传文件
    @Test
    public void putFileToHDFS() throws IOException, InterruptedException {
        // 1 创建配置信息对象,获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.0.100:9000/"), conf, "hadoop");
        //2.创建输入流
        FileInputStream inStream = new FileInputStream(new File("e:/hdfstest.txt"));
        //3.获取输出路径
        String putFileName = "hdfs://master:9000/user/test/out/hdfstest2.txt";
        Path outPath = new Path(putFileName);
        //4.创建输出流
        FSDataOutputStream outStream = fs.create(outPath);
        try {
            //5.流对接
            IOUtils.copyBytes(inStream,outStream,1024,true);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(inStream);
            IOUtils.closeStream(outStream);
        }
    }
    //IO流下载文件
    @Test
    public void getFileFromHDFS() throws IOException, InterruptedException {
        // 1 创建配置信息对象,获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.0.100:9000/"), conf, "hadoop");
        //2.创建读取路径
        String filename = "hdfs://master:9000/user/test/out/hdfstest1.txt";
        //3.创建读取path
        Path readPath = new Path(filename);
        //4.创建输入流
        FSDataInputStream inStream = fs.open(readPath);
        try {
            //5.流对接
            IOUtils.copyBytes(inStream,System.out,1024,true);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(inStream);
        }

    }
}
