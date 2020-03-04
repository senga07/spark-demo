package com.hlsijx.spark.sql.project.etl.yarn

import com.hlsijx.spark.sql.factory.SparkSqlFactory
import com.hlsijx.spark.sql.project.etl.LogETLLocalApp
import com.hlsijx.spark.sql.system.PathConfig

/**
  * 使用spark-submit在yarn执行
  * ./bin/spark-submit --class com.hlsijx.spark.project.etl.yarn.LogETLApp \
  * --name LogETLApp \
  * --master yarn \
  * --deploy-mode client \
  * --driver-memory 1g \
  * --executor-memory 1g \
  * --executor-cores 2 \
  * --queue thequeue \
  * /data/jar/spark-1.0-jar-with-dependencies.jar \
  * file:///data/log/access.20161111.log file:///data/log/clean \
  * --files ipDatabase.csv,ipRegion.xlsx
  */
object LogETLApp {

  def main(args: Array[String]): Unit = {

    if (args.length < 2){
      println("Usage : LogETLApp <BasePath> <FileName>")
      System.exit(1)
    }

    PathConfig.isOnYarn = true
    PathConfig.inputPath = args(0)
    PathConfig.outputPath = args(1)

    val sparkSession = SparkSqlFactory.createSpark("LogETLApp")

    //读取文本文件
    val logRDD = SparkSqlFactory.readTextFile(sparkSession, PathConfig.inputPath)

    //通过两次ETL后得到处理后的数据
    val dateFrame = LogETLLocalApp.etlStepTwo(sparkSession, LogETLLocalApp.etlStepOne(logRDD))

    //将数据写出到指定路径，以date字段进行分区，只输出一个文件
    SparkSqlFactory.writeParquetFile(dateFrame, "date", PathConfig.outputPath, 1)

    sparkSession.stop()
  }
}
