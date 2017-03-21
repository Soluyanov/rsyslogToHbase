package com.test.core.streaming
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{
  HFileOutputFormat2,
  TableInputFormat,
  LoadIncrementalHFiles
}
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka._
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.hadoop.fs.FSDataOutputStream
import java.io._
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.hadoop.hbase.{
  HColumnDescriptor,
  HTableDescriptor,
  HBaseConfiguration
}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Application used to write rsyslog messages to Hbase
  */
/**
  * Application used to write rsyslog messages to Hbase
  */
case class HBaseRecord(
                        col0: String,
                        col1: Boolean)

object HBaseRecord {
  def apply(i: Int): HBaseRecord = {
     val s = s"""row${"%03d".format(i)}"""
    HBaseRecord(s,
      i % 2 == 0)
  }
}

object Fake {
  val Log = Logger.getLogger(Fake.this.getClass().getSimpleName())
  val cat = s"""{
               |"table":{"namespace":"default", "name":"shcExampleTable", "tableCoder":"PrimitiveType"},
               |"rowkey":"key",
               |"columns":{
               |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
               |"col1":{"cf":"cf1", "col":"col1", "type":"boolean"}
               |}
               |}""".stripMargin

  def main(args: Array[String]) {

    if (args.length < 4) {
      System.err.println(
        "Usage: zkQuorum,  group, topics" +
          "numThreads, pathToStoreParquet, pathToStore")
      System.exit(1)
    }

    val Array(zkQuorum,
    group,
    topics,
    numThreads,
    pathToStoreParquet,
    pathToStore) = args

    val sparkConf = new SparkConf().setAppName("Fake")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val conf = HBaseConfiguration.create()
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    
     val data = (0 to 15).map { i =>
      HBaseRecord(i)
    }

     sc.parallelize(data).toDF.write.options(
        Map(HBaseTableCatalog.tableCatalog -> cat, HBaseTableCatalog.newTable -> "5"))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()


val data1 = (16 to 30).map { i =>
      HBaseRecord(i)
    }

    sc.parallelize(data1).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> cat, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
sc.stop()

  }
}


