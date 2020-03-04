package com.hlsijx.spark.sql.version_2

import org.apache.spark.sql.SparkSession

/**
  * SparkSession的使用，2.0以后SparkSession就是统一的入口
  */
object SparkSessionApp {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("SparkSessionApp").master("local[2]").getOrCreate()

    sparkSession.read.json(args(0)).show()

    sparkSession.stop()
  }
}
