package com.hlsijx.spark.sql.version_2

import org.apache.spark.sql.SparkSession

/**
  * DataSet的基本操作
  * DataSet比DataFrame功能更强大，能够将属性错误在编译时发现
  */
object DataSetApp {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("DataSetApp").master("local[2]").getOrCreate()

    val path = "./test-data/csv/people.csv"
    val dataFrame = sparkSession.read.option("header", "true").option("inferSchema", "true").csv(path)

    //知识点：利用隐式转换和as函数，将DataFrame转成DataSet
    import sparkSession.implicits._
    val dataSet = dataFrame.as[People]

    dataSet.map(people => people.name + people.age).show()

    sparkSession.stop()
  }

  case class People(name : String, age : Int, job : String)
}
