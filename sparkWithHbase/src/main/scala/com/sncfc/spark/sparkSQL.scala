package com.sncfc.spark

import java.io.File

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by 16060764 on 2016/12/28.
  */
object sparkSQL {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val path = new File(".").getCanonicalPath()
    //File workaround = new File(".");
    System.getProperties().put("hadoop.home.dir", path);
    new File("./bin").mkdirs();
    new File("./bin/winutils.exe").createNewFile();


    // 本地模式运行,便于测试
    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkSQLwithHbase")
    //创建Hbase 配置
    val hbaseconf = HBaseConfiguration.create()
    hbaseconf.set("hbase.zookeeper.quorum", "cdhslave03,cdhslave01,cdhslave02")
    hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseconf.set(TableInputFormat.INPUT_TABLE, "order_detail")
    // 创建 spark context
    val sc = new SparkContext(conf)
    val sqlcontext = new SQLContext(sc)
    import sqlcontext.implicits._
    //创建RDD
    val hbaseRDD = sc.newAPIHadoopRDD(
      hbaseconf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])



    // 将数据映射为表  也就是将 RDD转化为 dataframe schema
    val orders = hbaseRDD.map(r => (
      Bytes.toString(r._2.getValue(Bytes.toBytes("orderinfo"), Bytes.toBytes("userid"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("orderinfo"), Bytes.toBytes("paydate"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("orderinfo"), Bytes.toBytes("sendpay"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("moneyinfo"), Bytes.toBytes("orderamt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("moneyinfo"), Bytes.toBytes("orderyouhui")))
      )).toDF("userid", "paydate", "sendpay", "orderamt", "orderyouhui")

    orders.registerTempTable("order_info")
    orders.cache()

    val ordersum = orders.count()

    // 测试
    val df2 = sqlcontext.sql("SELECT userid,sum(orderamt)  FROM order_info group by userid")
    println("总共  " + ordersum + "个订单")
    df2.map(t => "用户: " + t(0) + "  总金额：" + "%20.0f".format(t(1))).collect().foreach(println)


  }
}
