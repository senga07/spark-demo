package com.hlsijx.spark.utils;

import com.hlsijx.spark.kafka.constants.HBaseProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * HBase工具类
 */
public class HBaseUtils {

    private Configuration configuration;
    private HBaseAdmin admin;

    private HBaseUtils(){
        configuration = new Configuration();
        configuration.set(HBaseProperties.zookeeperQuorum, "hlsijx:2181");
        configuration.set(HBaseProperties.rootdir, "hdfs://hlsijx:8020/hbase");
        try {
            admin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HBaseUtils instance = null;
    public static synchronized HBaseUtils getInstance(){
        if (instance == null){
            instance = new HBaseUtils();
        }
        return instance;
    }

    public void save(String tableName, String row, String family,
                     String qualifier, String value){
        HTable table = getTable(tableName);
        try {
            table.incrementColumnValue(Bytes.toBytes(row), Bytes.toBytes(family),
                    Bytes.toBytes(qualifier), Long.valueOf(value));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 新增一条数据到HBase
     */
    public void add(String tableName, String row, String family,
                    String qualifier, String value){
        HTable table = getTable(tableName);

        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 查询某一条记录
     */
    public String query(String tableName, String row, String family, String qualifier){
        HTable table = getTable(tableName);

        Get get = new Get(Bytes.toBytes(row));
        try {
            byte[] value = table.get(get).getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            if (value.length == 0){
                return "0";
            }
            return new String(value);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "0";
    }

    /**
     * 根据表名获取HTable
     */
    private HTable getTable(String tableName){
        HTable table = null;
        try {
            table = new HTable(configuration, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    public static void main(String[] args) throws IOException {

        String localHadoopUrl = "D:/application/hadoop-2.6.0-cdh5.15.1";
        System.setProperty("hadoop.home.dir", localHadoopUrl);
        System.setProperty("HADOOP_USER_NAME", "root");

//        getInstance().add(CommonConfig.table(), "20201111_22",CommonConfig.family(), CommonConfig.qualifier(), "20");
//        String value = getInstance().query(CommonConfig.table(), "20200306_128", CommonConfig.family(), CommonConfig.qualifier());
//        System.out.println(value);
    }
}

/**
 * 创建表：指定表名为`statistic_course`，列簇为`day_cid`
 * create 'statistic_course','day_cid'
 *
 * 查询表
 * scan 'statistic_course'
 *
 * 插入数据：表名，rowKey，列簇：列名，值
 * put 'statistic_course','20201111_22','day_cid:courseid','88'
 *
 * 带条件查询
 * get 'statistic_course','20201111_22'
 */
