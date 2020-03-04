package com.hlsijx.spark.sql.version_1

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SQLContext的使用
  * 执行命令：
  * spark-submit \
  * --class com.hlsijx.spark.version_1.SQLContextApp \
  * --master local[2] \
  * /data/spark-2.4.4-bin-2.6.0-cdh5.15.1/lib/spark-1.0.jar \
  * /data/spark-2.4.4-bin-2.6.0-cdh5.15.1/examples/src/main/resources/people.json
  */
object SQLContextApp {

  def main(args: Array[String]): Unit = {

    //创建相应的Context
    val sparkConfig = new SparkConf()//.setAppName("SQLContextApp").setMaster("local[2]")
    val sparkContext = new SparkContext(sparkConfig);
    val sqlContext = new SQLContext(sparkContext)

    //读取json数据
    val dataFrame = sqlContext.read.format("json").load(args(0))
    dataFrame.printSchema()
    dataFrame.show()

    //关闭资源
    sparkContext.stop()
  }
}
