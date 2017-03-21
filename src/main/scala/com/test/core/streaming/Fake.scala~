package com.test.core.streaming

import org.apache.spark._
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import twitter4j._

/**
  * Application used to write twitter messages to Hbase
  */
object Fake {

  val Log = Logger.getLogger(Fake.this.getClass().getSimpleName())
  def main(args: Array[String]) {
    if (args.length < 6) {
      System.err.println(
        "Usage: consumerKey, consumerSecret, accessToken" +
          "tokenSecret, tableName, userId")
      System.exit(1)
    }

    val Array(consumerKey,
              consumerSecret,
              accessToken,
              tokenSecret,
              tableName,
              userId) = args
    val sparkConf = new SparkConf().setAppName("Fake")
    val sc = new SparkContext(sparkConf)

    val config = new twitter4j.conf.ConfigurationBuilder()
      .setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(tokenSecret)
      .build

    val conf = HBaseConfiguration.create()
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    val myTable = new HTable(conf, tableName)

    /** Writes twitter message to Hbase */
    def putTweetToHbase(columnFamily: String,
                        columnName: String,
                        tweet: String) = {
      var p = new Put(System.currentTimeMillis().toString.getBytes())
      p.add(columnFamily.getBytes(), columnName.getBytes(), tweet.getBytes)
      myTable.put(p) //ask a question
      myTable.flushCommits()

    }

    def simpleStatusListener = new StatusListener() {
      def onStatus(status: Status) {
        println(status.getText)
        putTweetToHbase("cf", "column_name", status.getText)
      }
      def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
      def onTrackLimitationNotice(numberOfLimitedStatuses: Int) {}
      def onException(ex: Exception) { ex.printStackTrace }
      def onScrubGeo(arg0: Long, arg1: Long) {}
      def onStallWarning(warning: StallWarning) {}
    }

    val twitterStream = new TwitterStreamFactory(config).getInstance
    twitterStream.addListener(simpleStatusListener)
    twitterStream.filter(new FilterQuery(Array(userId.toLong)))

  }
}
