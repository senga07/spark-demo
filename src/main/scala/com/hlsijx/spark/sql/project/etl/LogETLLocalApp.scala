package com.hlsijx.spark.sql.project.etl

import com.hlsijx.spark.sql.factory.SparkSqlFactory
import com.hlsijx.spark.sql.system.{PathConfig}
import com.hlsijx.spark.utils.{DateUtils, IPUtils, RegExpUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * 原始日志进行etl
  */
object LogETLLocalApp {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSqlFactory.createSpark("LogETLApp")

    //读取文本文件
    val logRDD = SparkSqlFactory.readTextFile(sparkSession, PathConfig.inputPath)

    //通过两次ETL后得到处理后的数据
    val dateFrame = etlStepTwo(sparkSession, etlStepOne(logRDD))

    //将数据写出到指定路径，以date字段进行分区，只输出一个文件
    SparkSqlFactory.writeParquetFile(dateFrame, "date", PathConfig.outputPath, 1)

    sparkSession.stop()
  }


  /**
    * 第一步数据处理：原始数据 => 第一次etl后的数据
    */
  def etlStepOne(logRDD : RDD[String]) : RDD[String] = {

    logRDD.map(line => {
      val fields = line.split(" ")
      val ip = fields(0)
      val temp = fields(3) + " " + fields(4)
      val time = DateUtils.parse(temp.substring(temp.indexOf("[") + 1, temp.lastIndexOf("]")))
      val url = fields(11).replaceAll("\"", "")
      val traffic = fields(9)
      if (url.equals("-") ||
        !RegExpUtils.isPass(url, "[a-z:./]*video/[0-9]*") &&
          !RegExpUtils.isPass(url, "[a-z:./]*article/[0-9]*")) {
        "-"
      } else {
        time + "\t" + url + "\t" + traffic + "\t" + ip
      }
    }).filter(!_.equals("-"))
  }

  /**
    * 第二步数据处理：第一次etl后的数据 => 第二次etl后的数据
    */
  def etlStepTwo(sparkSession: SparkSession, logRDD : RDD[String]) = {

    //定义输出对象结构
    val structType = StructType(
      Array(
        StructField("url", StringType),
        StructField("cmsType", StringType),
        StructField("cmsId", LongType),
        StructField("traffic", LongType),
        StructField("ip", StringType),
        StructField("city", StringType),
        StructField("time", StringType),
        StructField("date", StringType)
      )
    )

    //数据转换
    val rdd = logRDD.map(lines => {

      try{
        val fileds = lines.split("\t")
        val time = fileds(0)
        val url = fileds(1)
        val traffic = fileds(2)
        val ip = fileds(3)

        val uri = url.replace("http://www.imooc.com/", "").replace("http://m.imooc.com/", "")
        val temp = uri.split("/")
        Row(url, temp(0), temp(1).toLong, traffic.toLong, ip, IPUtils.getCity(ip), time, DateUtils.format(time))
      } catch {
        case e : Exception => Row("", "", 0.toLong, 0.toLong, "", "", "", "")
      }
    })

    sparkSession.createDataFrame(rdd, structType)
  }
}
