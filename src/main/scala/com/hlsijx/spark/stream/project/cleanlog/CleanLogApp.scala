package com.hlsijx.spark.stream.project.cleanlog

import com.hlsijx.spark.utils.DateUtils
import com.hlsijx.spark.utils.DateUtils.{formatOne, formatTwo}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 实时数据清洗
  */
object CleanLogApp {

  def main(args: Array[String]): Unit = {

    if (args.length != 2){
      System.err.print("Usage: CleanLogApp <brokerList> <topic>")
      System.exit(1)
    }

    val Array(brokerList, topic) = args

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("CleanLogApp")
    val ssc = new StreamingContext(sparkConf, Seconds(60))

    val kafkaParams = Map("metadata.broker.list" -> brokerList)
    val topics = topic.split(",").toSet
    val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    //132.87.29.124	  2020-03-06 05:56:10	  "GET /class/146.html HTTP/1.1"  200	  https://www.baidu.com/s?wd=spark sql实战
    val lines = directKafkaStream.map(_._2).map(line => getClickLog(line)).filter(log => log.courseId > 0)
    lines.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def getClickLog(line : String): ClickLog ={
    val fields = line.split("\t")
    val ip = fields(0)
    val time = DateUtils.parse(fields(1), formatOne, formatTwo)
    val uri = fields(2)
    val statusCode = fields(3).toInt
    val referer = fields(4)

    ClickLog(ip, time, getCourseId(uri), statusCode, referer)
  }

  //"GET /class/146.html HTTP/1.1"
  def getCourseId(uri : String): Int ={
    val temp = uri.split(" ")(1)
    if (temp.startsWith("/class/")){
      val a = temp.split("/")(2)
      a.substring(0, a.lastIndexOf(".")).toInt
    } else {
      0
    }
  }
}
