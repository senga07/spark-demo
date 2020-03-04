package com.hlsijx.spark.stream.wordcount

import com.hlsijx.spark.{CommonConfig}
import com.hlsijx.spark.stream.factory.SparkStreamFactory

/**
  * Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
  * To run this on your local machine, you need to first run a Netcat server
  *    `$ nc -lk 9999`
  *
  * Key Func:socketTextStream
  */
object NetworkWordCount {

  def main(args: Array[String]): Unit = {

    val ssc = SparkStreamFactory.createStreamingContext("NetworkWordCount")

    val lines = ssc.socketTextStream(CommonConfig.hostname, CommonConfig.port)

    val wordCount = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    wordCount.print()

    SparkStreamFactory.startStreamingJob(ssc)
  }
}
