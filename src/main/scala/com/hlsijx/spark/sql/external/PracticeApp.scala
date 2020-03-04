package com.hlsijx.spark.sql.external

import org.apache.spark.sql.SparkSession

/**
  * 实战练习：
  * 在hive中存在emp表，在mysql中dept表，将两张表的数据进行join
  */
object PracticeApp {

  def main(args: Array[String]): Unit = {

    //创建时要加上enableHiveSupport
    val sparkSession = SparkSession.builder().appName("PracticeApp").master("local[2]")
      .enableHiveSupport().getOrCreate()

    //获取hive中的emp表
    sparkSession.sql("use h_demo")
    val emp = sparkSession.table("emp")

    //获取mysql中的dept表
    val dept = sparkSession.read.format("jdbc")
      .option("url", "jdbc:mysql://127.0.0.1:3306")
      .option("dbtable", "demo.dept")
      .option("user", "root")
      .option("password", "Hihuuapp0@cheyoubao").load()

    //进行表关联
    emp.join(dept, emp.col("deptno") === dept.col("deptno")).show()

    sparkSession.stop()
  }
}
