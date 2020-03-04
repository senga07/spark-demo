package com.hlsijx.spark.sql.utils

import com.ggstar.util.ip.IpHelper


/**
  * IP工具类
  */
object IPUtils {

  def getCity(ip : String): String ={
    try{
      IpHelper.findRegionByIp(ip)
    }catch {
      case e : Exception => "-"
    }
  }
}
