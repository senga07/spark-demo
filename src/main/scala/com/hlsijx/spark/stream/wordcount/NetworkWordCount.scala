package com.hlsijx.spark.stream.wordcount

import com.hlsijx.spark.CommonConfig
import com.hlsijx.spark.stream.factory.SparkStreamFactory
import org.apache.spark.streaming.Seconds

/**
  * Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
  * To run this on your local machine, you need to first run a Netcat server
  *    `$ nc -lk 9999`
  * 实时监听9999端口的输入内容，并进行词频统计
  *
  * Key Func:socketTextStream（）
  */
object NetworkWordCount {

  def main(args: Array[String]): Unit = {

    val ssc = SparkStreamFactory.createStreamingContext("NetworkWordCount")

    val lines = ssc.socketTextStream(CommonConfig.hostname, CommonConfig.port)

    val wordCount = lines.flatMap(_.split(" ")).map((_, 1))//.reduceByKey(_ + _)
      //Reduce last 30 seconds of data, every 10 seconds 每过10s统计一次最后30s内的数据
      .reduceByKeyAndWindow((a:Int,b:Int) => a + b, Seconds(30), Seconds(10))

    wordCount.print()

    SparkStreamFactory.startStreamingJob(ssc)
  }


}
