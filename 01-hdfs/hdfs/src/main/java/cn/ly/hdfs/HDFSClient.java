package cn.ly.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/**
 * api操作hdfs
 */
public class HDFSClient {

    FileSystem fs;

    @Before
    public void initHDFS() throws IOException, InterruptedException {
        //设置hadoop环境变量
        System.setProperty("hadoop.home.dir", "E:\\hadoop-2.7.3.tar\\hadoop-2.7.3");
        String uri = "hdfs://192.168.0.100:9000/";
        // 1 创建配置
        // 创建一个新的Configuration实例时，会加载core-default.xml与core-site.xml，若还有其它配置，则调用addResource()继续添加。
        Configuration conf = new Configuration();
        System.out.println(conf.get("fs.defaultFS"));
        System.out.println(conf.get("dfs.replication"));
        // 2 设置参数
        // 参数优先级： 1、客户端代码中设置的值  2、classpath下的用户自定义配置文件 3、然后是服务器的默认配置
        //configuration.set("fs.defaultFS", "hdfs://hadoop102:8020");
        //conf.set("dfs.replication", "3");
        // 3 获取文件系统
        fs = FileSystem.get(URI.create(uri), conf, "hadoop");
    }
    @After
    public void afterHDFS() throws IOException {
        fs.close();
    }

    //列出hdfs上 /目录下的所有文件
    @Test
    public void testListStatus() throws IOException {
        FileStatus[] statuses = fs.listStatus(new Path("/"));
        for (FileStatus status : statuses) {
            System.out.println(status);
        }
    }
    //创建目录
    @Test
    public void mkdirAtHDFS() throws IOException {
        fs.mkdirs(new Path("hdfs://master:9000/user/test/output"));
    }

    //删除文件夹，如果是非空文件夹，参数2必须给值true
    @Test
    public void deleteAtHDFS() throws IOException {
        fs.delete(new Path(("hdfs://master:9000/user/atguigu")),true);
    }
    //上传文件
    @Test
    public void putFileToHDFS() throws IOException {
        //本地文件
        Path src = new Path("e:/hdfstest.txt");
        //hdfs目标路径
        Path dst = new Path("hdfs://master:9000/user/test/output");
        //上传
        fs.copyFromLocalFile(src,dst);
    }

    //下载文件
    @Test
    public void getFileFromHDFS() throws IOException {
        Path src = new Path("hdfs://master:9000/user/test/output/hdfstest.txt");
        Path dst = new Path("e:/hellocopy.txt");
        // 2 下载文件
        // boolean delSrc 指是否将原文件删除
        // boolean useRawLocalFileSystem 是否开启文件效验
        fs.copyToLocalFile(false, src, dst, true);
    }
}
