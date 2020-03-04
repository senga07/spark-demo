package com.hlsijx.spark.sql.external

import com.hlsijx.spark.sql.system.SparkSqlProperties
import org.apache.spark.sql.SparkSession

/**
  * 官网地址：https://spark.apache.org/docs/latest/sql-data-sources-parquet.html
  * 外部数据源之Parquet文件操作
  * 在Spark中，默认的数据源类型就是Parquet
  * 1.x版本内置的数据源：json、parqust、jdbc
  * 2.x版本时增加了csv
  * 其他外部的数据可以通过这个网站寻找：https://spark-packages.org/
  */
object ParquetApp {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("ParquetApp").master("local[2]").getOrCreate()

    /**
      * 加载Parquet文件,标准写法
      * sparkSession.read.format("parquet").load(filepath)
      * 以下为简写，使用parquet后，就不用load
      */
    val user = sparkSession.read.parquet("file:///data/spark-2.4.4-bin-2.6.0-cdh5.15.1/examples/src/main/resources/users.parquet")
    user.show()

    /**
      * 输出为json文件,标准写法
      * df.write.format("json").save(filepath)
      * 以下为简写，使用json后，就不用save
      */
    user.select("name", "favorite_color").write.json("file:///data/output")

    //在生产中要注意配置这个值，含义是在shuffle或aggregations时partitions的值
    sparkSession.sqlContext.getConf(SparkSqlProperties.partitions)

    sparkSession.stop()
  }
}
