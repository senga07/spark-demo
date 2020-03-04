package com.hlsijx.spark.sql.rdd

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * DataFrame与RDD的交互操作方式一
  * 用适场景：知道表的列及类型
  * 特点：代码量少，利用反射实现
  */
object Connection_1App {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("RDD2DataFrameApp").master("local[2]").getOrCreate()

    //加载txt文件为RDD
    val rdd = sparkSession.sparkContext.textFile("./test-data/txt/people.txt")

    //隐式转换，将RDD转换为DataFrame
    import sparkSession.implicits._
    val dataFrame = rdd.map(_.split(","))
      .map(lines => People(lines(0).replaceAll(" ", ""),
        lines(1).replaceAll(" ", "").toInt,
        lines(2).replaceAll(" ", ""))).toDF()

    querySql(sparkSession, dataFrame)

    sparkSession.stop()
  }

  case class People(name : String, age : Int, city : String)

  /**
    * 两种方式实现实现sql查询
    * 1、DataFrame的API操作
    * 2、创建临时表，直接写sql
    */
  def querySql(sparkSession: SparkSession, dataFrame: DataFrame): Unit = {
    //方式一：操作DataFrame的API
    dataFrame.filter(dataFrame.col("age") > 20).show()

    //方式二：创建临时表
    dataFrame.createOrReplaceTempView("employees")
    sparkSession.sql("select * from employees where age > 20").show()
  }
}
