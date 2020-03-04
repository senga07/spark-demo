package com.hlsijx.spark.sql.rdd

import com.hlsijx.spark.sql.rdd.Connection_1App.querySql
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * DataFrame与RDD的交互操作方式二
  * 用适场景：不知道表的列及类型
  * 特点：代码量多，函数式编程
  */
object Connection_2App {

  def main(args: Array[String]): Unit = {

    //设置入口点
    val sparkSession = SparkSession.builder().appName("Connection_2App").master("local[2]").getOrCreate()

    //加载people.txt文件
    val people = sparkSession.sparkContext.textFile("./test-data/txt/people.txt")

    //要点一：转换为Row
    val rdd = people.map(_.split(",")).map(lines => Row(lines(0).replaceAll(" ", ""),
        lines(1).replaceAll(" ", ""), lines(2).replaceAll(" ", "")))

    //要点二：定义结构StructType
    val structType = StructType(
      Array(

        StructField("name", StringType, nullable = true),
        StructField("age", StringType, nullable = true),
        StructField("city", StringType, nullable = true)
      )
    )

    //要点三：创建DataFrame并传入rdd和structType
    val dataFrame = sparkSession.createDataFrame(rdd, structType)

    //执行sql查询
    querySql(sparkSession, dataFrame)

    //关闭
    sparkSession.stop()
  }
}
