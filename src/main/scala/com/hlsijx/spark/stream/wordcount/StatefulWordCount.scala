package com.hlsijx.spark.stream.wordcount

import com.hlsijx.spark.{CommonConfig}
import com.hlsijx.spark.stream.factory.SparkStreamFactory

/**
  * Counts words in UTF8 encoded
  * Counting words continuously
  * Key Func:updateStateByKey
  */
object StatefulWordCount {

  def main(args: Array[String]): Unit = {

    val ssc = SparkStreamFactory.createStreamingContext("StatefulWordCount")

    /**
      * 1、必须要设置checkponit
      * checkponit需要配置目录，生产环境建议放到HDFS上
      */
    ssc.checkpoint(CommonConfig.tmp_dir)

    val lines = ssc.socketTextStream(CommonConfig.hostname, CommonConfig.port)

    val pairs = lines.flatMap(_.split(" ")).map((_, 1))

    /**
      * 2、Define the state - The state can be an arbitrary data type
      * 使用到了隐式转换
      */
    val runningCounts = pairs.updateStateByKey[Int](updateFunction _)

    runningCounts.print()

    SparkStreamFactory.startStreamingJob(ssc)
  }

  /**
    * 3、Define the state update function - Specify with a function how to update the state using the previous state and the new values from an input stream.
    * @param newValues    新加入的数据
    * @param runningCount 已经运行过的数据
    * @return             返回两者之和
    */
  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    // add the new values with the previous running count to get the new count
    val newCount = newValues.sum + runningCount.getOrElse(0)
    Some(newCount)
  }
}
