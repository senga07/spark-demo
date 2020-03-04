package com.hlsijx.spark.sql.project.stat_city

import com.hlsijx.spark.sql.factory.SparkSqlFactory
import com.hlsijx.spark.sql.project.stat_city.dao.VideoAccessCityStatDao
import com.hlsijx.spark.sql.system.PathConfig
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window

/**
  * 日志分析实战案例之按城市统计TopN的课程
  */
object StatCityApp {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSqlFactory.createSpark("StatCityApp")

    //读取文件
    val dateFrame = SparkSqlFactory.readParquetFile(sparkSession, PathConfig.outputPath)

    //进行统计
    val result = videoAccessStatByCity(sparkSession, dateFrame)

    //保存到数据库
    VideoAccessCityStatDao.addVideoAccessCityStat(result)

    sparkSession.stop()
  }

  private def videoAccessStatByCity(sparkSession: SparkSession, dateFrame: DataFrame) : Dataset[Row] = {

    import org.apache.spark.sql.functions._
    import sparkSession.implicits._
    val result = dateFrame
      .filter($"cmsType" === "video" && $"date" === "20161110")
      .groupBy("cmsId", "date", "city")
      .agg(count("cmsId").as("total"))


    result.select(result("cmsId"), result("city"),result("date"), result("total"),
      //窗口函数
      row_number().over(
        Window.partitionBy("city").orderBy($"total".desc)
      ).as("rank")
    ).filter($"rank" <= 3 and ($"city" !== null))

//      +-----+------+--------+-----+----+
//      |cmsId|city  |date    |total|rank|
//      +-----+------+--------+-----+----+
//      |14540|杭州市|20170511|22435|1   |
//      |14322|杭州市|20170511|11151|2   |
//      |14390|杭州市|20170511|11110|3   |
//      |14540|null  |20170511|22058|1   |
//      |14704|null  |20170511|11219|2   |
//      |4000 |null  |20170511|11182|3   |
//      |14540|朝阳区|20170511|22270|1   |
//      |4600 |朝阳区|20170511|11271|2   |
//      |14390|朝阳区|20170511|11175|3   |
//      |14540|合肥市|20170511|22149|1   |
//      +-----+------+--------+-----+----+
  }
}
