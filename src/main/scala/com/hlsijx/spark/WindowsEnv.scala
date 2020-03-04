package com.hlsijx.spark

/**
  * 本地window环境参数设置
  */
object WindowsEnv {

  def setWinEnv(): Unit ={
    val localHadoopUrl = "D:/application/hadoop-2.6.0-cdh5.15.1"
    System.setProperty("hadoop.home.dir", localHadoopUrl)
    System.setProperty("HADOOP_USER_NAME", "root")
  }
}
