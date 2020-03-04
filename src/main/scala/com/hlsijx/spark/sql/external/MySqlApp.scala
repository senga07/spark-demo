package com.hlsijx.spark.sql.external

import org.apache.spark.sql.SparkSession

/**
  * 外部数据源之MySql基本操作
  * 官网地址：https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
  */
object MySqlApp {

  def main(args: Array[String]): Unit = {

    /**
      * 1、引入jar包，启动时指定driver-class-path
      * spark-shell --driver-class-path /data/spark-2.4.4-bin-2.6.0-cdh5.15.1/jars/mysql-connector-java-5.1.27-bin.jar
      * 2、依次执行以下代码
      */
    val sparkSession = SparkSession.builder().appName("MySqlApp").master("local[2]").getOrCreate()
    val jdbcDF = sparkSession.read.format("jdbc")
      .option("url", "jdbc:mysql://127.0.0.1:3306")
      .option("dbtable", "demo.TBLS")
      .option("user", "root")
      .option("password", "Hihuuapp0@cheyoubao").load()
    jdbcDF.show()
    sparkSession.stop()
  }
}
