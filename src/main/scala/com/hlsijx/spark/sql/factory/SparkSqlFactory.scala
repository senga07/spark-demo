package com.hlsijx.spark.sql.factory

import com.hlsijx.spark.WindowsEnv
import com.hlsijx.spark.sql.system.{PathConfig, SparkSqlProperties}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object SparkSqlFactory {

  /**
    * 创建一个SparkSession
    * @param name 任务名称
    * @return     SparkSession
    */
  def createSpark(name : String) : SparkSession ={

    if (!PathConfig.isOnYarn){
      WindowsEnv.setWinEnv()
      SparkSession.builder().appName(name).master("local[2]")
        .config(SparkSqlProperties.typeInfer, value = false).getOrCreate()
    } else {
      SparkSession.builder().config(SparkSqlProperties.typeInfer, value = false).getOrCreate()
    }
  }

  /**
    * 读取文本文件
    * @param spark 入口点
    * @param path  文件路径
    * @return      RDD[String]
    */
  def readTextFile(spark: SparkSession, path : String) : RDD[String] ={

    spark.sparkContext.textFile(path)
  }

  /**
    * 输出为文本文件
    * @param rdd        输出数据
    * @param outputPath 指定路径
    */
  def writeTextFile(rdd : RDD[String], outputPath : String): Unit ={

    rdd.saveAsTextFile(outputPath)
  }

  /**
    * 读取parquet文件
    * @param sparkSession 入口点
    * @param inputPath    文件路径
    * @return
    */
  def readParquetFile(sparkSession: SparkSession , inputPath : String) ={

    sparkSession.read.parquet(inputPath)
  }

  /**
    * 输出为parquet文件，使用覆盖模式
    * @param dataFrame     dateFrame
    * @param partitionCol  分区字段
    * @param outputPath    输出路径
    * @param numPartitions 通过设置coalesce的值，来控制输出文件个数（调优点）
    */
  def writeParquetFile(dataFrame : DataFrame, partitionCol : String, outputPath : String, numPartitions : Int) ={

    dataFrame
      .coalesce(numPartitions)
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy(partitionCol)
      .parquet(outputPath)
  }
}
