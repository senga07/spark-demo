package com.hlsijx.spark.stream

import com.hlsijx.spark.sql.system.WindowsEnv
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
  * To run this on your local machine, you need to first run a Netcat server
  *    `$ nc -lk 9999`
  */
object NetworkWordCount {

  def main(args: Array[String]): Unit = {

    WindowsEnv.setWinEnv()

    val hostname = "hlsijx"
    val port = 9999

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val lines = ssc.socketTextStream(hostname, port)
    val wordCount = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
