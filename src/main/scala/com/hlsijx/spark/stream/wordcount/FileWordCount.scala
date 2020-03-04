package com.hlsijx.spark.stream.wordcount

import com.hlsijx.spark.WindowsEnv
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Counts words in UTF8 encoded
  * Files must be written to the monitored directory by "moving" them from another
  * location within the same file system. File names starting with . are ignored.
  */
object FileWordCount {

  def main(args: Array[String]): Unit = {

    WindowsEnv.setWinEnv()

    val sparkConf = new SparkConf().setAppName("FileWordCount").setMaster("local")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.textFileStream("./test-data")

    val wordCount = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
