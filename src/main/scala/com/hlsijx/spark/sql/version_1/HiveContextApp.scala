package com.hlsijx.spark.sql.version_1

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * HiveContext的使用
  * 打包到服务器后，还需要将hive_site.xml文件放到$SPARK_HOME/conf下
  * 启动metastore服务
  * 执行命令：
  * spark-submit \
  * --class com.hlsijx.spark.version_1.HiveContextApp \
  * --master local[2] \
  * --jars /data/hive-1.1.0-cdh5.15.1/lib/mysql-connector-java-5.1.27-bin.jar \
  * /data/spark-2.4.4-bin-2.6.0-cdh5.15.1/lib/spark-1.0.jar
  */
object HiveContextApp {
  def main(args: Array[String]): Unit = {

    //创建相应的Context
    val sparkConfig = new SparkConf()
    val sparkContext = new SparkContext(sparkConfig)
    val hiveContext = new HiveContext(sparkContext)

    //读取emp表
    hiveContext.table("h_demo.emp").show()

    //关闭资源
    sparkContext.stop()
  }
}
