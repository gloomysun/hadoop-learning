package cn.ly.hbaseAPI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseAPI_new {
    static String ZK_CONNECT_KEY = "hbase.zookeeper.quorum";
    static String ZK_CONNECT_VALUE = "vm1";
    private static Connection conn = null;
    private static Admin admin = null;

    public static void main(String[] args) throws IOException {
        getConnection();
        getAdmin();
        HbaseAPI_new api = new HbaseAPI_new();
//        api.getAllTables();
//        api.createTable("test_table", "cf1", "cf2");
        // api.descTable("test_table");
//        api.putData("test_table","r1","cf1","c1","v1");
//        api.putData("test_table","r1","cf1","c2","v2");
//        api.putData("test_table","r1","cf2","c2","v2");
//        api.putData("test_table", "r1", "cf1", "c1", "v11", System.currentTimeMillis());

//        Result result = api.getResult("test_table", "r1");
//        System.out.println(api.getResult("test_table", "r1"));
//        System.out.println(new String(result.getRow()) + "\t" + new String(result.getValue("cf1".getBytes(), "c1".getBytes())));

//        System.out.println(api.getResult("test_table", "r2", "cf1"));
//        System.out.println(api.getResult("test_table", "r2", "cf1", "c2"));

//        System.out.println(api.getResultByVersion("test_table","r1","cf1","c1",2));
       api.getResultByVersion("t1","row1","f1","name",3);

        api.getResultByVersion("test_table","r1","cf1","c1",3);


    }

    /**
     * 获取连接
     */
    public static Connection getConnection() {
        //创建一个可以用来管理hbase的conf对象
        Configuration conf = HBaseConfiguration.create();
        //设置当前的程序寻找hbase在哪
        conf.set(ZK_CONNECT_KEY, ZK_CONNECT_VALUE);
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 获取管理员对象
     */
    public static Admin getAdmin() {
        try {
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return admin;
    }

    /**
     * 查询所有表和列族信息
     */
    public void getAllTables() throws IOException {
        //获取列族的描述信息
        HTableDescriptor[] listTables = admin.listTables();
        for (HTableDescriptor listTable : listTables) {
            //转化为表名
            String tbName = listTable.getTableName().getNameAsString();
            //获取列的描述信息
            HColumnDescriptor[] columnFamilies = listTable.getColumnFamilies();
            System.out.println("tableName:" + tbName);
            for (HColumnDescriptor columnFamily : columnFamilies) {
                //获取列族的名字
                String columnFamilyName = columnFamily.getNameAsString();
                System.out.println("\t" + "columnFamily:" + columnFamilyName);
            }
        }
    }

    /**
     * 创建表
     *
     * @param tableName    表名
     * @param columnFamily 列族
     */
    public void createTable(String tableName, String... columnFamily) throws IOException {
        TableName name = TableName.valueOf(tableName);
        //判断表是否存在
        if (admin.tableExists(name)) {
            System.out.println("talbe已经存在");
        } else {
            //表的列族描述
            HTableDescriptor htd = new HTableDescriptor(name);
            //向表中添加列族
            for (String str : columnFamily) {
                HColumnDescriptor hcd = new HColumnDescriptor(str);
                htd.addFamily(hcd);
            }
            //创建表
            admin.createTable(htd);
            //判断表是否创建成功
            if (admin.tableExists(name)) {
                System.out.println(String.format("table%s创建成功", tableName));
            } else {
                System.out.println(String.format("talbe%s创建失败", tableName));
            }
        }
    }

    /**
     * 查看表的列族属性
     *
     * @param tableName 表名
     * @throws IOException
     */
    public void descTable(String tableName) throws IOException {
        //转换为表名
        TableName name = TableName.valueOf(tableName);
        //判断表是否存在
        if (admin.tableExists(name)) {
            //获取列族信息描述
            HTableDescriptor htd = admin.getTableDescriptor(name);
            HColumnDescriptor[] hcds = htd.getColumnFamilies();
            System.out.println(hcds.length);
            for (HColumnDescriptor hct : hcds) {
                System.out.println(hct);
            }
        } else {
            System.out.println("table不存在");
        }
    }

    /**
     * 判断表是否存在
     */
    public boolean existTable(String tableName) throws IOException {
        TableName name = TableName.valueOf(tableName);
        return admin.tableExists(name);
    }

    /**
     * 删除表
     *
     * @param tableName
     * @throws IOException
     */
    public void dropTable(String tableName) throws IOException {
        TableName name = TableName.valueOf(tableName);
        //判断表是否存在
        if (admin.tableExists(name)) {
            //判断表是否处于可用状态
            boolean tableEnabled = admin.isTableEnabled(name);
            if (tableEnabled) {
                admin.disableTable(name);
            }
            //删除表
            admin.deleteTable(name);
            //判断表是否存在
            if (admin.tableExists(name)) {
                System.out.println("删除失败");
            } else {
                System.out.println("删除成功");
            }
        } else {
            System.out.println("table不存在");
        }
    }

    /**
     * 添加列族
     *
     * @param tableName
     * @param columnFamily
     */
    public void addFamily(String tableName, String... columnFamily) throws IOException {
        TableName name = TableName.valueOf(tableName);
        //判断表是否存在
        if (admin.tableExists(name)) {
            //判断表是否可用
            if (admin.isTableEnabled(name)) {
                admin.disableTable(name);
            }
            HTableDescriptor htd = admin.getTableDescriptor(name);
            for (String cf : columnFamily) {
                HColumnDescriptor hcd = new HColumnDescriptor(cf);
                htd.addFamily(hcd);
            }
            admin.modifyTable(name, htd);
        } else {
            System.out.println("表不存在");
        }
    }


    /**
     * 添加数据
     *
     * @param tableName
     * @param rowKey
     * @param colunmFamily
     * @param column
     * @param value
     */
    public void putData(String tableName, String rowKey, String colunmFamily, String column, String value) throws IOException {
        TableName name = TableName.valueOf(tableName);
        if (!admin.tableExists(name)) {
            HTableDescriptor htd = admin.getTableDescriptor(name);
            HColumnDescriptor hcd = new HColumnDescriptor(colunmFamily);
            htd.addFamily(hcd);
            admin.createTable(htd);
        }
        Table table = conn.getTable(name);
        Put put = new Put(rowKey.getBytes());
        put.addColumn(colunmFamily.getBytes(), column.getBytes(), value.getBytes());
        table.put(put);
        table.close();
    }

    public void putData(String tableName, String rowKey, String columnFamily, String column, String value, long timestamp) throws IOException {
        TableName name = TableName.valueOf(tableName);
        if (!admin.tableExists(name)) {
            HTableDescriptor htd = admin.getTableDescriptor(name);
            HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);
            htd.addFamily(hcd);
            admin.createTable(htd);
        }
        Table table = conn.getTable(name);
        Put put = new Put(rowKey.getBytes());
        put.addColumn(columnFamily.getBytes(), column.getBytes(), timestamp, value.getBytes());
        table.put(put);
        table.close();
    }

    /**
     * 根据rowKey查询数据
     *
     * @param tableName
     * @param rowKey
     * @return
     * @throws IOException
     */
    public Result getResult(String tableName, String rowKey) throws IOException {
        Result result;
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            Table table = conn.getTable(name);
            Get get = new Get(rowKey.getBytes());
            result = table.get(get);
        } else {
            result = null;
        }
        return result;
    }

    public Result getResult(String tableName, String rowKey, String columnFamily) throws IOException {
        Result result;
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            Table table = conn.getTable(name);
            Get get = new Get(rowKey.getBytes());
            get.addFamily(columnFamily.getBytes());
            result = table.get(get);
        } else {
            result = null;
        }
        return result;
    }

    public Result getResult(String tableName, String rowKey, String columnFamily, String column) throws IOException {
        Result result;
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            Table table = conn.getTable(name);
            Get get = new Get(rowKey.getBytes());
            get.addColumn(columnFamily.getBytes(), column.getBytes());
            result = table.get(get);
        } else {
            result = null;
        }
        return result;
    }

    public Result getResultByVersion(String tableName, String rowKey, String columnFamily, String column, int versions) throws IOException {
        Result result;
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            Table table = conn.getTable(name);
            Get get = new Get(rowKey.getBytes());
            get.addColumn(columnFamily.getBytes(), column.getBytes());
            get.setMaxVersions(versions);
            result = table.get(get);
            for(Cell cell:result.listCells()){
                System.out.println(Bytes.toString(CellUtil.cloneFamily(cell))+"\t"+Bytes.toString(CellUtil.cloneQualifier(cell))+"\t"+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        } else {
            result = null;
        }
        return result;
    }
}
