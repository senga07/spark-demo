package com.hlsijx.spark.stream.wordcount

import com.hlsijx.spark.stream.factory.SparkStreamFactory

/**
  * Counts words in UTF8 encoded
  * Files must be written to the monitored directory by "moving" them from another
  * location within the same file system. File names starting with . are ignored.
  * 实时读取test-data文件夹下的text文件，并进行词频统计
  *
  * Key Func:textFileStream
  */
object FileWordCount {

  def main(args: Array[String]): Unit = {

    val ssc = SparkStreamFactory.createStreamingContext("FileWordCount")

    val lines = ssc.textFileStream("./test-data")

    val wordCount = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    wordCount.print()

    SparkStreamFactory.startStreamingJob(ssc)
  }
}
