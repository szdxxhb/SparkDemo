package com.xhb.hbase;

/**
 * Created by 16060764 on 2016/12/19.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public   class HbaseUtils {
    /*静态初始化*/
    public static Configuration conf = null;

     static {
         conf = HBaseConfiguration.create();
         conf.set("hbase.zookeeper.quorum", "cdhslave03,cdhslave01,cdhslave02");
         conf.set("hbase.zookeeper.property.clientPort", "2181");
     }

    /*
        * 创建表
        * @tableName 表名
        * @family 列族列表
        */
    public  void creatTable(String tableName, String[] family) {
        Connection connection=null;
        Admin admin = null;
        try {
            connection= ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
        TableName tablename = TableName.valueOf(tableName);
        //设置列族
        HTableDescriptor hbaseTableDesc = new HTableDescriptor(tablename);
        for (int i = 0; i < family.length; i++) {
            hbaseTableDesc.addFamily(new HColumnDescriptor(family[i]));
        }

        try {
            if (admin.tableExists(tablename)) {
                System.out.println("table Exists!create table Failure");
            } else {
                admin.createTable(hbaseTableDesc);
                System.out.println("create table Success!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /*
        * 删除表
        * @tableName 表名
        */
    public  void dropTable(String tableName ) throws Exception {
        Connection connection=null;
        Admin admin = null;
        try {
            connection= ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
        TableName tablename = TableName.valueOf(tableName);
        try {
            if (admin.tableExists(tablename)) {
                System.out.println("table Exists! start to drop table ");
                admin.disableTable(tablename);
                admin.deleteTable(tablename);

                if (admin.tableExists(tablename)) {
                    System.out.println("delete table Failure!");
                    throw new Exception("delete table Failure!") ;
                } else {
                    System.out.println("delete table Success! ");
                }
            } else {
                System.out.println("table Not  Exists");
                throw new Exception("table Not  Exists,delete table Failure!") ;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
         * 为表添加数据（适合知道有多少列族的固定表）
         * @rowKey rowKey
         * @tableName 表名
         * @column1 第一个列族列表
         * @value1 第一个列的值的列表
         * @column2 第二个列族列表
         * @value2 第二个列的值的列表
         */
    public   void addData(String rowKey, String tableName,
                               String[] column1, String[] value1, String[] column2, String[] value2)
            throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
        HTable table = new HTable(conf, Bytes.toBytes(tableName));// HTabel负责跟记录相关的操作如增删改查等//
        // 获取表
        HColumnDescriptor[] columnFamilies = table.getTableDescriptor() // 获取所有的列族
                .getColumnFamilies();

        for (int i = 0; i < columnFamilies.length; i++) {
            String familyName = columnFamilies[i].getNameAsString(); // 获取列族名
            if (familyName.equals("orderinfo")) { // orderinfo列族put数据
                for (int j = 0; j < column1.length; j++) {
                    put.add(Bytes.toBytes(familyName),
                            Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
                }
            }
            if (familyName.equals("moneyinfo")) { // moneyinfo列族put数据
                for (int j = 0; j < column2.length; j++) {
                    put.add(Bytes.toBytes(familyName),
                            Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));
                }
            }
        }
        table.put(put);
        System.out.println("add data Success!");
    }


}