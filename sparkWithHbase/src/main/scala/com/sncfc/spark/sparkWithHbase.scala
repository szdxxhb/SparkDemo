package com.sncfc.spark

import java.io.File
import java.util.NavigableMap

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{Row, RowFactory, SQLContext}
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

  def resultToList(tableValues: Tuple2[ImmutableBytesWritable, Result]): ListBuffer[Row] = {
    val tup = tableValues._2
    val list: ListBuffer[Row] = new ListBuffer[Row]

    for(u <- tup.rawCells()){
      val row: String = Bytes.toString(CellUtil.cloneRow(u))
      val family: String = Bytes.toString(CellUtil.cloneFamily(u))
      val column: String = Bytes.toString(CellUtil.cloneQualifier(u))
      val value: String = Bytes.toString(CellUtil.cloneValue(u))
      list.+=:(RowFactory.create(row, family, column, value))

      /**
      val row: String = Bytes.toString(CellUtil.cloneRow(u))
       if(row == rowKey){
         val family: String = Bytes.toString(CellUtil.cloneFamily(u))
         val column: String = Bytes.toString(CellUtil.cloneQualifier(u))
         val value: String = Bytes.toString(CellUtil.cloneValue(u))
         list.+=:(RowFactory.create(row, family, column, value))
       }
        */
    }
    list
  }

  def cfToList(cfValues: NavigableMap[Array[Byte], Array[Byte]]): ListBuffer[Row] = {
    val list: ListBuffer[Row] = new ListBuffer[Row]
    val keySet = cfValues.navigableKeySet()
    val iterator = keySet.iterator()
    while(iterator.hasNext){
      val keyValue = iterator.next()
      val column: String = Bytes.toString(keyValue)
      val value:String = Bytes.toString(cfValues.get(keyValue))
      list.+=:(RowFactory.create(column, value))
    }
    list
  }

  case class mapTypeTable(row: String, family: String, column: String, value: String)

  def queryDataFromHBaseTable(sc: SparkContext, sqlContext: SQLContext, tabName: String): Unit ={
    config.set(TableInputFormat.INPUT_TABLE, tabName)
    val rdd = sc.newAPIHadoopRDD(config, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    val resultRDD = rdd.map(tup => resultToList(tup))
    import sqlContext.implicits._
    val df = resultRDD.flatMap(x => x.toList).map{
      case Row(row: String, family: String, column: String, value: String) =>
        mapTypeTable(row, family, column, value) }.toDF()
    df.show()
  }

  case class mapTypeColumnFamily(column: String, value: String)

  def queryDataFromHBaseColumnFamily(sc: SparkContext,
                                     sqlContext: SQLContext,
                                     tabName: String,
                                     cfName: String): Unit ={
    config.set(TableInputFormat.INPUT_TABLE, tabName)
    val rdd = sc.newAPIHadoopRDD(config,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    val resultRDD = rdd.map(tup => tup._2)
      .map(cfRDD => cfRDD.getFamilyMap(Bytes.toBytes(cfName)))
      .map(cfValue => cfToList(cfValue))
    import sqlContext.implicits._
    val df = resultRDD.flatMap(x => x.toList).map{
      case Row(column: String, value: String) => mapTypeColumnFamily(column, value) }.toDF()
    df.show()
  }

  case class mapTypeColumnValue(value: String)
  def queryDataFromHBaseColumnValue(sc: SparkContext,
                                    sqlContext: SQLContext,
                                    tabName: String,
                                    cfName: String,
                                    colName: String): Unit ={
    config.set(TableInputFormat.INPUT_TABLE, tabName)
    val rdd = sc.newAPIHadoopRDD(config,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    val resultRDD = rdd.map(tup => tup._2)
      .map(cfRDD => cfRDD.getValue(Bytes.toBytes(cfName), Bytes.toBytes(colName)))
    val values = resultRDD.map(value => RowFactory.create(Bytes.toString(value)))
    import sqlContext.implicits._
    val df = values.map{
      case Row(value: String) => mapTypeColumnValue(value) }.toDF()
    df.show()
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
    val cfName = "orderinfo"
    val colName = "paydate"

    //queryDataFromHBaseTable(sc, sqlContext, tableName)
    //queryDataFromHBaseColumnFamily(sc, sqlContext, tableName, cfName)
    queryDataFromHBaseColumnValue(sc, sqlContext, tableName, cfName, colName)


  }

}
