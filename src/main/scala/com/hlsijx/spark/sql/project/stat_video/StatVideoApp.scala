package com.hlsijx.spark.sql.project.stat_video

import com.hlsijx.spark.sql.factory.SparkFactory
import com.hlsijx.spark.sql.project.stat_video.dao.VideoAccessStatDao
import com.hlsijx.spark.sql.system.PathConfig
import org.apache.spark.sql._

/**
  * 日志分析实战案例之统计TopN的课程
  */
object StatVideoApp {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkFactory.createSpark("StatVideoApp")

    //读取文件
    val dateFrame = SparkFactory.readParquetFile(sparkSession, PathConfig.outputPath)

    //进行统计
    val result = videoAccessStat(sparkSession, dateFrame, 0)

    //保存到数据库
    VideoAccessStatDao.addVideoAccessStat(result)

    sparkSession.stop()
  }

  /**
    * 统计慕课网日志TopN的课程的访问量
    * 有三种实现方式，结果都是一样的
    */
  private def videoAccessStat(sparkSession: SparkSession, dataFrame: DataFrame, methodType : Int) ={
    if (methodType == 0){
      videoAccessStatByAPI1(sparkSession, dataFrame)
    } else if (methodType == 1){
      videoAccessStatByAPI2(sparkSession, dataFrame)
    } else {
      videoAccessStatBySQL(sparkSession, dataFrame)
    }
  }

  private def videoAccessStatByAPI1(sparkSession: SparkSession, dateFrame: DataFrame) : Dataset[Row] = {
    /*
      注意：
      1、$"paramName"需要引入   `import sparkSession.implicits._`
      2、count方法需要引入      `import org.apache.spark.sql.functions._`
    */
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._
    dateFrame.filter($"cmsType" === "video" && $"date" === "20161110")
      .groupBy("cmsId", "date").agg(count("cmsId").as("total")).orderBy($"total".desc)
  }

  private def videoAccessStatByAPI2(sparkSession: SparkSession, dateFrame: DataFrame) : Dataset[Row]  = {

    import sparkSession.implicits._
    dateFrame.filter("cmsType = 'video' and date = '20161110'")
      .groupBy("cmsId", "date").count().orderBy($"count".desc)
  }

  private def videoAccessStatBySQL(sparkSession: SparkSession, dateFrame: DataFrame) : DataFrame ={

    dateFrame.createOrReplaceTempView("access")
    val sql = "select cmsId, date, count(1) total from access where cmsType = 'video' and date = '20161110' " +
      "group by cmsId,date order by total desc"
    sparkSession.sql(sql)
  }
}
