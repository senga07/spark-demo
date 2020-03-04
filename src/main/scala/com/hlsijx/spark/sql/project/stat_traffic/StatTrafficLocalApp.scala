package com.hlsijx.spark.sql.project.stat_traffic

import com.hlsijx.spark.sql.factory.SparkFactory
import com.hlsijx.spark.sql.project.stat_traffic.dao.VideoAccessTrafficStatDao
import com.hlsijx.spark.sql.system.PathConfig
import org.apache.spark.sql._

/**
  * 日志分析实战案例之按流量统计TopN的课程
  */
object StatTrafficLocalApp {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkFactory.createSpark("StatTrafficApp")

    processOn(sparkSession)
  }

  def processOn(sparkSession: SparkSession): Unit ={
    //读取文件
    val dateFrame = SparkFactory.readParquetFile(sparkSession, PathConfig.outputPath)

    //进行统计
    val result = videoAccessStatByTraffic(sparkSession, dateFrame)

    //保存到数据库
    VideoAccessTrafficStatDao.addVideoAccessTrafficStat(result)

    sparkSession.stop()
  }

  def videoAccessStatByTraffic(sparkSession: SparkSession, dateFrame: DataFrame) : Dataset[Row] = {

    import org.apache.spark.sql.functions._
    import sparkSession.implicits._
    dateFrame.filter($"cmsType" === "video" && $"date" === "20161110").groupBy("cmsId", "date").agg(sum("traffic").as("traffic_sum")).orderBy($"traffic_sum".desc)

//      +-----+--------+-----------+
//      |cmsId|date    |traffic_sum|
//      +-----+--------+-----------+
//      |14540|20170511|55454898   |
//      |14390|20170511|27895139   |
//      |4500 |20170511|27877433   |
//      |4000 |20170511|27847261   |
//      |14623|20170511|27822312   |
//      |4600 |20170511|27777838   |
//      |14704|20170511|27737876   |
//      |14322|20170511|27592386   |
//      +-----+--------+-----------+
  }
}
