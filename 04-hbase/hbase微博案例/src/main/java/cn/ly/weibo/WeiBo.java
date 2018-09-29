package cn.ly.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class WeiBo {
    private Logger logger = Logger.getLogger(WeiBo.class);
    //获取配置
    private static Configuration conf;
    //创建微博namespace
    private static final String NS_WEIBO = "ns_weibo";
    //微博内容表的表名
    private static final byte[] TABLE_CONTENT = Bytes.toBytes("ns_weibo:content");
    //微博用户关系表的表名
    private static final byte[] TABLE_RELATION = Bytes.toBytes("ns_weibo:realation");
    //微博收件箱表的表名
    private static final byte[] TABLE_INBOX = Bytes.toBytes("ns_weibo:inbox");

    //设置hbase的配置信息——hbase是强依赖于hdfs和zookeeper的
    //与hbase-site.xml中一致
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", "hdfs://master:9000/hbase");                              //设置hbase数据在hdfs中存放位置
        conf.set("hbase.zookeeper.quorum", "master:2181,slave1:2181,slave2:2181");       //配置zookeeper
    }

    /**
     * 初始化命名空间，可以想象成文件夹
     */
    public void initNameSpace() {
        logger.info("正在初始化namespace");
        HBaseAdmin admin = null;
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = (HBaseAdmin) connection.getAdmin();
            NamespaceDescriptor weibo = NamespaceDescriptor
                    .create(NS_WEIBO)
                    .addConfiguration("creator", "gloomysun")
                    .addConfiguration("create_time", "" + System.currentTimeMillis())
                    .build();
            admin.createNamespace(weibo);
            logger.info(NS_WEIBO + "命名空间创建成功");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != admin) {
                try {
                    admin.close();
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 创建微博内容表
     * 表结构
     * TableName :　ns_weibo:content
     * rowKey:用户ID_时间戳
     * ColumnFamily : info
     * ColumnQualifier : content
     * Value : 微博内容(文字内容;图片URL;视频URL;语音URL)
     * Versions : 1
     * 此时的版本为1指的是：
     * 对于同一rowKey中的数据只能存放一个版本
     * 也就是说：当对一个rowKey进行二次添加数据时，则为覆盖
     */
    public void createTableContent() {
        logger.info("正在初始化content表");
        HBaseAdmin admin = null;
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = (HBaseAdmin) connection.getAdmin();
            HTableDescriptor contentTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_CONTENT));
            HColumnDescriptor infoColumnDescriptor = new HColumnDescriptor(Bytes.toBytes("info"));
            //设置块缓存
            infoColumnDescriptor.setBlockCacheEnabled(true);
            //设置块缓存大小
            infoColumnDescriptor.setBlocksize(2097152);
            //设置压缩方式
            //infoColumnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
            //设置版本确界
            infoColumnDescriptor.setMaxVersions(1);
            infoColumnDescriptor.setMinVersions(1);
            contentTableDescriptor.addFamily(infoColumnDescriptor);
            admin.createTable(contentTableDescriptor);
            logger.info("content表创建成功");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != admin) {
                try {
                    admin.close();
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 创建用户关系表
     * TableName :　ns_weibo:relation
     * rowKey:当前用户ID
     * ColumnFamily : attends fans      两个列族——两个HColumnDescriptor
     * ColumnQualifier : 用户ID
     * Value : 无实际含义的空字符串
     * Versions : 1
     */
    public void createTableRelation() {
        logger.info("正在初始化relation表");
        HBaseAdmin admin = null;
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = (HBaseAdmin) connection.getAdmin();
            HTableDescriptor relationTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_RELATION));
            //关注的人列族
            HColumnDescriptor attendsColumnDescriptor = new HColumnDescriptor(Bytes.toBytes("attends"));
            //设置块缓存
            attendsColumnDescriptor.setBlockCacheEnabled(true);
            //设置块缓存大小
            attendsColumnDescriptor.setBlocksize(2097152);
            //设置压缩方式
            //attendColumnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
            //设置版本确界
            attendsColumnDescriptor.setMaxVersions(1);
            attendsColumnDescriptor.setMinVersions(1);
            //粉丝列族
            HColumnDescriptor fansColumnDescriptor = new HColumnDescriptor(Bytes.toBytes("attend"));
            fansColumnDescriptor.setBlockCacheEnabled(true);
            fansColumnDescriptor.setBlocksize(2097152);
            //fansColumnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
            fansColumnDescriptor.setMaxVersions(1);
            fansColumnDescriptor.setMinVersions(1);

            relationTableDescriptor.addFamily(attendsColumnDescriptor);
            relationTableDescriptor.addFamily(fansColumnDescriptor);
            admin.createTable(relationTableDescriptor);
            logger.info("relation表创建成功");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != admin) {
                try {
                    admin.close();
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 创建微博收件箱表
     * TableName :　ns_weibo:inbox
     * rowKey:当前用户ID
     * ColumnFamily : info
     * ColumnQualifier : 关注人的用户ID
     * Value : 关注人的微博rowKey
     * Versions : 1000
     * 同一个rowKey中的数据可以存放1000个版本
     * 若想获取最近的5条微博，则可以通过Get类中的setMaxVersions来设置获取的版本数，新API中使用的是readVersions(int versions)方法
     */
    public void createTableInbox() {
        logger.info("正在调用初始化inbox表");
        HBaseAdmin admin = null;
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = (HBaseAdmin) connection.getAdmin();
            HTableDescriptor inboxTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_INBOX));
            HColumnDescriptor infoColumnDescriptor = new HColumnDescriptor(Bytes.toBytes("info"));
            //设置块缓存
            infoColumnDescriptor.setBlockCacheEnabled(true);
            //设置块缓存大小
            infoColumnDescriptor.setBlocksize(2097152);
            //设置版本确界
            infoColumnDescriptor.setMaxVersions(1000);
            infoColumnDescriptor.setMinVersions(1000);
            inboxTableDescriptor.addFamily(infoColumnDescriptor);
            admin.createTable(inboxTableDescriptor);
            logger.info("inbox表创建成功");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != admin) {
                try {
                    admin.close();
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 发布微博
     *
     * @param uid     用户id
     * @param content 微博内容
     *                <p>
     *                a.向content表中添加一条记录
     *                b.向微博收件箱表中加入微博的rowKey
     */
    public void publishContent(String uid, String content) {
        logger.info(uid + "正在调用publishContent方法发布微博");
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            //a.向content表中添加一条数据
            Table contentTable = connection.getTable(TableName.valueOf(TABLE_CONTENT));
            //a.1 封装要添加的数据
            Put put = new Put((uid + "_" + System.currentTimeMillis()).getBytes());
            put.addColumn("info".getBytes(), "content".getBytes(), content.getBytes());
            //a.2 添加数据
            contentTable.put(put);
            //b.向微博收件箱表中加入数据
            //b.1 获取用户关系表
            Table realationTable = connection.getTable(TableName.valueOf(TABLE_RELATION));
            //b.2从relation表中获取粉丝id
            Get get = new Get(uid.getBytes());
            get.addFamily("attends".getBytes());
            Result result = realationTable.get(get);
            if (result.listCells().size() <= 0) return;
            List<Put> puts = new ArrayList<Put>();
            Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));
            for (Cell cell : result.listCells()) {
//                System.out.println(CellUtil.cloneQualifier(cell));
                //获取粉丝的id
                byte[] fansId = CellUtil.cloneQualifier(cell);
                //想微博收件箱表中加入数据，rowKey:粉丝id
                Put fansPut = new Put(fansId);
                fansPut.addColumn("info".getBytes(), "uid".getBytes(), content.getBytes());
                puts.add(fansPut);
            }
            inboxTable.put(puts);
            logger.info(uid + "微博发布成功");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != connection) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 添加关注用户
     *
     * @param uid
     * @param attends a.用户关系表给当前用户添加关注人
     *                b.用户关系表给所有关注人添加粉丝
     *                c.收件箱表给当前用户添加关注人发布的微博
     */
    public void addAttends(String uid, String... attends) {
        //参数过滤
        if (attends == null || attends.length <= 0 || uid == null || uid.length() <= 0) {
            return;
        }
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            List<Put> fansPuts = new ArrayList<Put>();
            //a.在用户关系表中,对当前的用户id进行添加关注操作
            //a.1 获取用户关系表
            Table relationTable = connection.getTable(TableName.valueOf(TABLE_RELATION));
            //a.1.添加新关注的好友
            Put attendPut = new Put(uid.getBytes());
            for (String attend : attends) {
                attendPut.addColumn("attends".getBytes(), attend.getBytes(), "".getBytes());
                //b.为被关注的人添加粉丝
                Put fansPut = new Put(attend.getBytes());
                fansPut.addColumn("fans".getBytes(), uid.getBytes(), "".getBytes());
                fansPuts.add(fansPut);
            }
            relationTable.put(attendPut);
            relationTable.put(fansPuts);
            //c.微博收件箱添加关注的用户发布的微博内容（content）的 rowkey
            Table contentTable = connection.getTable(TableName.valueOf(TABLE_CONTENT));
            Scan scan = new Scan();
            //用于存放取出来的关注的人所发布的微博的rowKey
            List<byte[]> rowKeys = new ArrayList<byte[]>();
            for (String attend : attends) {
                //过滤扫描 rowkey，即：前置位匹配被关注的人的 uid_
                RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(attend + "_"));
                //为扫描对象指定过滤规则
                scan.setFilter(rowFilter);
                //通过扫描对象得到scanner
                ResultScanner scanner = contentTable.getScanner(scan);
                //遍历扫描出来的结果集
                Iterator<Result> iterator = scanner.iterator();
                if (iterator.hasNext()) {
                    //取出扫描的每一个结果
                    Result result = iterator.next();
                    for (Cell cell : result.listCells()) {
                        rowKeys.add(CellUtil.cloneRow(cell));
                    }
                }
            }
            //c.2.将取出的rowKeys插入当前用户的收件箱表中
            Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));
            List<Put> inboxPutList = new ArrayList<Put>();

            Put inboxPut = new Put(uid.getBytes());
            for (byte[] rowKey : rowKeys) {
                String attendUID = Bytes.toString(rowKey).split("_")[0]; //截取被关注的人的id
                String timestamp = Bytes.toString(rowKey).split("_")[1]; //截取时间戳
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}