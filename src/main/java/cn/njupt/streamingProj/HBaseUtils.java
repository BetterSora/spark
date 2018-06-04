package cn.njupt.streamingProj;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * HBase操作工具类：Java工具类简易采用单列模式封装
 */
public class HBaseUtils {
    private HBaseAdmin admin = null;
    private Configuration configuration = null;
    private static volatile HBaseUtils instance = null;

    private HBaseUtils() {
        configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", "hadoop:2181");
        configuration.set("hbase.rootdir", "hdfs://hadoop:8020/hbase");

        try {
            admin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static HBaseUtils getInstance() {
        if (instance == null) {
            synchronized (HBaseUtils.class) {
                if (instance == null) {
                    instance = new HBaseUtils();
                }
            }
        }

        return instance;
    }

    /**
     * 根据表名获取HTable实例
     */
    public HTable getTable(String tableName) {
        HTable table = null;

        try {
            table = new HTable(configuration, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return table;
    }

    /**
     * 添加一条记录到HBase表
     * @param tableName HBase表名
     * @param rowkey  HBase表的rowkey
     * @param cf HBase表的columnfamily
     * @param column HBase表的列
     * @param value  写入HBase表的值
     */
    public void put(String tableName, String rowkey, String cf, String column, String value) {
        HTable table = getTable(tableName);

        Put put = new Put(rowkey.getBytes());
        put.add(cf.getBytes(), column.getBytes(), value.getBytes());

        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据表名和输入条件获取HBase的记录数
     */
    public Map<String, Long> query(String tableName, String condition) throws Exception {
        Map<String, Long> map = new HashMap<>();

        HTable table = getTable(tableName);
        String cf = "info";
        String qualifier = "click_count";

        Scan scan = new Scan();

        Filter filter = new PrefixFilter(Bytes.toBytes(condition));
        scan.setFilter(filter);

        ResultScanner rs = table.getScanner(scan);
        for(Result result : rs) {
            String row = Bytes.toString(result.getRow());
            long clickCount = Bytes.toLong(result.getValue(cf.getBytes(), qualifier.getBytes()));
            map.put(row, clickCount);
        }

        return  map;
    }

    public static void main(String[] args) throws Exception {
        //HTable table = HBaseUtils.getInstance().getTable("course_clickcount");
        //System.out.println(table.getName().getNameAsString());

       /* String tableName = "course_clickcount" ;
        String rowkey = "20171111_88";
        String cf = "info" ;
        String column = "click_count";
        String value = "2";

        HBaseUtils.getInstance().put(tableName, rowkey, cf, column, value);*/

        Map<String, Long> map = HBaseUtils.getInstance().query("course_clickcount" , "20180513");

        for(Map.Entry<String, Long> entry: map.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }
}
