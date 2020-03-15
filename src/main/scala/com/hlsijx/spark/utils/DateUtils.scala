package com.hlsijx.spark.utils

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 日期工具类
  */
object DateUtils {

  def parse(time : String, beforeFormat : String, afterFormat : String): String ={
    /**
      * 建议使用FastDataFormat替代SimpleDateFormat
      * 因为FastDataFormat是线程不安全的
      */
    val inputFormat = FastDateFormat.getInstance(beforeFormat)
    val inputTime = inputFormat.parse(time).getTime

    val outputFormat = FastDateFormat.getInstance(afterFormat)
    outputFormat.format(new Date(inputTime))
  }

  /**
    * 将时间格式转换
    * eg：10/Nov/2016:00:01:02 +0800 ==> 2016-11-10 00:01:02
    * @param time
    */
  def parse(time : String): String ={
    /**
      * 建议使用FastDataFormat替代SimpleDateFormat
      * 因为FastDataFormat是线程不安全的
      */
    val inputFormat = FastDateFormat.getInstance(formatThree, Locale.ENGLISH)
    val inputTime = inputFormat.parse(time).getTime

    val outputFormat = FastDateFormat.getInstance(formatOne)
    outputFormat.format(new Date(inputTime))
  }

  /**
    * 日期格式化
    * @param time 时间 yyyy-MM-dd HH:mm:ss
    * @return 格式后日期 yyyyMMdd
    */
  def format(time : String): String = {

    time.substring(0, 10).replace("-", "")
  }
  def main(args: Array[String]): Unit = {
    print(parse("2020-03-06 05:56:10", formatOne, formatTwo))
  }

  val formatOne = "yyyy-MM-dd HH:mm:ss"
  val formatTwo = "yyyyMMddHHmmss"
  val formatThree = "dd/MMM/yyyy:HH:mm:ss Z"
}
