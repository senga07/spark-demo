package com.hlsijx.spark.stream.wordcount

import com.hlsijx.spark.CommonConfig
import com.hlsijx.spark.stream.factory.SparkStreamFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

/**
  * Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
  * To run this on your local machine, you need to first run a Netcat server
  *    `$ nc -lk 9999`
  * 实时监听9999端口的输入内容，并进行词频统计
  *
  * Key Func:socketTextStream
  */
object NetworkWordCount {

  def main(args: Array[String]): Unit = {

    val ssc = SparkStreamFactory.createStreamingContext("NetworkWordCount")

    val lines = ssc.socketTextStream(CommonConfig.hostname, CommonConfig.port)

    countBySql(lines)

    SparkStreamFactory.startStreamingJob(ssc)
  }

  /**
    * 使用reduce算子实现词频统计
    * Window Operation:reduceByKeyAndWindow
    */
  def countByReduce(lines: DStream[String]): Unit ={

    val wordCount = lines.flatMap(_.split(" ")).map((_, 1))//.reduceByKey(_ + _)
      //Reduce last 30 seconds of data, every 10 seconds 每过10s统计一次最后30s内的数据
      .reduceByKeyAndWindow((a:Int,b:Int) => a + b, Seconds(30), Seconds(10))

    wordCount.print()
  }

  /**
    * Spark Stream整合Spark SQL实现词频统计
    */
  def countBySql(lines: DStream[String]): Unit ={

    lines.foreachRDD { rdd =>

      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      // Convert RDD[String] to DataFrame
      val wordsDataFrame = rdd.toDF("word")

      // Create a temporary view
      wordsDataFrame.createOrReplaceTempView("words")

      // Do word count on DataFrame using SQL and print it
      val wordCountsDataFrame = spark.sql("select word, count(*) as total from words group by word")

      wordCountsDataFrame.show()
    }
  }

}
