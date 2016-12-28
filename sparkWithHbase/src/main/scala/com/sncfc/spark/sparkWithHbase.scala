package com.sncfc.spark

import java.io.File

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by 16060764 on 2016/12/27.
  */
object sparkWithHbase {
  //Hbase Config
  val config = HBaseConfiguration.create()
  //zookeeper 端口配置
  config.set("hbase.zookeeper.property.clientPort", "2181")
  //zookeeper 集群地址
  config.set("hbase.zookeeper.quorum", "cdhslave01,cdhslave02,cdhslave03")
  //hbase Master  server:port查看CDH集群管理  HBase Master 端口 hbase.master.port
  config.set("hbase.master", "cdhmaster:60000")
  //加载Hbase配置文件
  config.addResource("..\\files\\hbase-site.xml")



  def queryDataFromHBaseTable(sc: SparkContext, sqlContext: SQLContext, tableName: String): Unit = {
    //查询表名
    config.set(TableInputFormat.INPUT_TABLE, tableName)
    //构造RDD
    val conn = ConnectionFactory.createConnection(config)
    val userTable = TableName.valueOf(tableName)
    val tableDescr = new HTableDescriptor(userTable)
    val table = conn.getTable(userTable)
    //查询某条数据
/*    val g = new Get("0000185301".getBytes)
    val result = table.get(g)
    val value = Bytes.toString(result.getValue("orderinfo".getBytes,"paydate".getBytes))
    println("GET 0000185301 :"+value)*/

    val rdd = sc.newAPIHadoopRDD(config, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //打印条数
   /* val count: Long =rdd.count()
    println(count ) */

    




  }

  def main(args: Array[String]): Unit = {

    val path = new File(".").getCanonicalPath()
    //File workaround = new File(".");
    System.getProperties().put("hadoop.home.dir", path);
    new File("./bin").mkdirs();
    new File("./bin/winutils.exe").createNewFile();

    val sparkConf = new SparkConf().setAppName("Sncfc_Spark_with_Hbase")
    sparkConf.setMaster("local")
    sparkConf.set("spark.executor.memory", "128M")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    //定义表相关信息
    val tableName = "order_detail"


    queryDataFromHBaseTable(sc ,sqlContext ,tableName)


  }

}
