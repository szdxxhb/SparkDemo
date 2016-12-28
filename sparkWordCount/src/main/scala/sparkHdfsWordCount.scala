import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by 16060764 on 2016/12/23.
  */
object sparkHdfsWordCount {

  def readfromHDFS(sc: SparkContext): Unit = {
    val inputRDD=sc.textFile("hdfs://cdhmaster:8020/user/cfc/inputfile/workcount.txt")
    val wordCounts= inputRDD.flatMap(line => line.split(" ")).map( word => (word, 1)).reduceByKey((a, b) => a + b)
    wordCounts.collect().foreach(
      e=>{val (key,value)=e
        println("key="+key +":value="+value)
      }
    )
    println("count success")

    wordCounts.saveAsTextFile("hdfs://cdhmaster:8020/user/cfc/outputfile/workcount20161223.txt")
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SparkWithHDFSWordCount")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    readfromHDFS( sc)
  }

}
