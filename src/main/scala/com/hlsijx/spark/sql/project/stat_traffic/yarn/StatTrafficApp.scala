package com.hlsijx.spark.sql.project.stat_traffic.yarn

import com.hlsijx.spark.sql.factory.SparkFactory
import com.hlsijx.spark.sql.project.stat_traffic.StatTrafficLocalApp
import com.hlsijx.spark.sql.system.PathConfig

/**
  * 日志分析实战案例之按流量统计TopN的课程
  */
object StatTrafficApp {

  def main(args: Array[String]): Unit = {

    if (args.length < 1){
      println("Usage : StatTrafficApp <InputPath>")
      System.exit(1)
    }

    PathConfig.isOnYarn = true
    PathConfig.outputPath = args(0)

    val sparkSession = SparkFactory.createSpark("StatTrafficApp")

    StatTrafficLocalApp.processOn(sparkSession)
  }
}
