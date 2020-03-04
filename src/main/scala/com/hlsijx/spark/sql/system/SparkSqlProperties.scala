package com.hlsijx.spark.sql.system

/**
  * spark属性常量
  */
object SparkSqlProperties {

  /**
    * 是否开启类型自动推断
    */
  val typeInfer = "spark.sql.sources.partitionColumnTypeInference.enabled"

  /**
    * 设置并行度，默认值为200
    */
  val partitions = "spark.sql.shuffle.partitions"
}
