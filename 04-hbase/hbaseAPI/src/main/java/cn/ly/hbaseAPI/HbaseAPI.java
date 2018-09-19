package cn.ly.hbaseAPI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseAPI {
    private static Configuration conf;
    private static HBaseAdmin admin;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "vm1,vm2,vm3");
        try {
            admin = new HBaseAdmin(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表
     *
     * @param tableName
     * @param columnFamily
     * @throws IOException
     */
    public static void createTable(String tableName, String... columnFamily) throws IOException {
        if (admin.tableExists(tableName)) {
            System.out.println(tableName + "已经存在");
        } else {
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //创建多个列族
            for (String cf : columnFamily) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            admin.createTable(descriptor);
            System.out.println(tableName + "创建成功");
        }
    }

    /**
     * 删除表
     *
     * @param tableName
     * @throws IOException
     */
    public static void dropTable(String tableName) throws IOException {
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println(tableName + "删除成功");
        } else {
            System.out.println(tableName + "不存在");
        }
    }

    /**
     * 插入数据
     *
     * @param tableName
     * @param rowKey
     * @param colunmFamily
     * @param column
     * @param value
     * @throws IOException
     */
    public static void addRowData(String tableName, String rowKey, String colunmFamily, String column, String value) throws IOException {
        if (admin.tableExists(tableName)) {
            //创建HTable对象
            HTable hTable = new HTable(conf, tableName);
            Put put = new Put(rowKey.getBytes());
            put.add(colunmFamily.getBytes(), column.getBytes(), value.getBytes());
            hTable.put(put);
            hTable.close();
            System.out.println("插入数据成功");
        } else {
            System.out.println("表不存在");
        }
    }

    public static void deleteMultiRow(String tableName, String... rows) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        List<Delete> deleteList = new ArrayList<Delete>();
        for (String row : rows) {
            deleteList.add(new Delete(row.getBytes()));
        }
        hTable.delete(deleteList);
        hTable.close();
        System.out.println("删除多行成功");
    }

    public static void getAllRows(String tableName) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        //得到用于扫描region的对象
        Scan scan = new Scan();
        //使用htable得到resultScanner实现类的对象
        ResultScanner resultScanner = hTable.getScanner(scan);
        for (Result result : resultScanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println("rowKey:" + new String(CellUtil.cloneRow(cell)) +
                        "\tcolumnFamily:" + new String(CellUtil.cloneFamily(cell)) +
                        "\tcolumn:" + new String(CellUtil.cloneQualifier(cell)) +
                        "\tvalue:" + new String(CellUtil.cloneValue(cell)));
            }
        }

    }

    public static void main(String[] args) throws IOException {
        String tableName = "testTable";
        String[] colunmFamily = new String[]{"cf1", "cf2"};
        createTable(tableName, colunmFamily);

//        dropTable(tableName);

//        addRowData(tableName, "row1001", "cf1", "lie1", "cf1的值啊");

        getAllRows(tableName);
    }
}
