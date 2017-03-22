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

/**
  * Application used to write rsyslog messages to Hbase
  */
object Fake {

  val Log = Logger.getLogger(Fake.this.getClass().getSimpleName())

  case class RsyslogMessage(logMessage: String, date: java.sql.Date)
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

    /**
      * Writes messages in files and specifies the behavior, when target
      * file exists or not
      */
    def writeLogFiles(facilityLevel: String, msg: String) = {
      val path = new Path(pathToStore + "/" + facilityLevel)
      def defineOutputStream(fsDataOutputStream: FSDataOutputStream) = {
        val bufferedWriter = new BufferedWriter(
          new OutputStreamWriter(fsDataOutputStream))
        bufferedWriter.write(msg + "\n")
        bufferedWriter.close()
      }

      def append() = defineOutputStream(hdfs.append(path))

      def create() = defineOutputStream(hdfs.create(path))

      if (hdfs.exists(path)) append() else create()

    }

    /** Saves messages in parquet files*/
    def saveRddAsParquet(rdd: org.apache.spark.rdd.RDD[String], path: String) = {
      val utilDate = new java.util.Date()
      val date = new java.sql.Date(utilDate.getTime)
      val df = rdd.map(x => RsyslogMessage(x, date)).toDF()
      def createParquet() =
        df.write
          .partitionBy("date")
          .parquet(pathToStoreParquet)

      def appendParquet() =
        df.write
          .partitionBy("date")
          .mode(org.apache.spark.sql.SaveMode.Append)
          .parquet(pathToStoreParquet)
      if (hdfs.exists(new org.apache.hadoop.fs.Path(path))) appendParquet()
      else createParquet()

    }

    /** Defines message, when Kafka is a source */
    def defineMessageKafka(message: String) = message.split("\t")(4)

    /** Defines time generated, when Kafka is a source */
    def defineTimeKafka(message: String) = message.split("\t")(1)

    val conf = HBaseConfiguration.create()
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    val tableName = "t1"
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    val myTable = new HTable(conf, tableName)

    val job = Job.getInstance(conf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoad(job, myTable)

    val messages =
      KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    messages.print()

    messages.foreachRDD(rdd => {

      val bulk = rdd.map(x => {

        val kv: KeyValue = new KeyValue(Bytes.toBytes(defineTimeKafka(x)),
                                        "cf".getBytes(),
                                        "message".getBytes,
                                        defineMessageKafka(x).getBytes())
        (new ImmutableBytesWritable(Bytes.toBytes(defineTimeKafka(x))), kv)
      })

      bulk.saveAsNewAPIHadoopFile("/tmp/Hfiles",
                                  classOf[ImmutableBytesWritable],
                                  classOf[KeyValue],
                                  classOf[HFileOutputFormat2],
                                  conf)

      val bulkLoader = new LoadIncrementalHFiles(conf)
      bulkLoader.doBulkLoad(new Path("/tmp/Hfiles"), myTable)

      saveRddAsParquet(rdd, pathToStoreParquet)

    })

    val words = messages.map(x => (x.split("\t")(0).split('.')(1), 1))
    val filtered = words.filter(x => x._1 == "info")
    filtered.print()
    val windowedWordCounts = filtered.reduceByKeyAndWindow(
      (a: Int, b: Int) => a + b,
      Seconds(30),
      Seconds(30))
    windowedWordCounts.print()

    windowedWordCounts.foreachRDD(rdd => {
      rdd
        .collect()
        .foreach(infoCounts => {
          writeLogFiles("infoCounts", infoCounts._2.toString)
        })
    })

    Log.error("DEBUG info:" + zkQuorum)
    sys.ShutdownHookThread({
      println("Ctrl+C")
      try {
        ssc.stop(stopSparkContext = true, stopGracefully = true)
      } catch {
        case e: Throwable =>
          println("exception on ssc.stop(true, true) occured")

      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
