package com.hlsijx.spark.stream.project.cleanlog.statistic.dao

import com.hlsijx.spark.CommonConfig
import com.hlsijx.spark.stream.project.cleanlog.domain.{StatisticCourse}
import com.hlsijx.spark.utils.HBaseUtils

import scala.collection.mutable.ListBuffer

object StatisticCourseDao {

  def save(list : ListBuffer[StatisticCourse]){
    val instance = HBaseUtils.getInstance()
    for (ele <- list){
      instance.add(CommonConfig.table, ele.dayCourseid, CommonConfig.family, CommonConfig.qualifier, ele.count.toString)
    }
  }
}
