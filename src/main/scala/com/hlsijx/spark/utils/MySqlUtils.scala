package com.hlsijx.spark.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.hlsijx.spark.sql.system.PathConfig

object MySqlUtils {

  /**
    * 获取数据库连接
    * @return
    */
  def getConnection() : Connection = {
    var url : String = null
    if (PathConfig.isOnYarn){
      url = "jdbc:mysql://localhost:3306/demo?user=root&password=Hihuuapp0@cheyoubao"
    } else {
      url = "jdbc:mysql://localhost:3306/cybx?user=root"
    }
    DriverManager.getConnection(url)
  }

  /**
    * 释放数据库连接等资源
    */
  def release(connection : Connection, pstmt : PreparedStatement) : Unit = {
    try{
      if (pstmt != null){
        pstmt.close()
      }
    } catch {
      case e : Exception => e.printStackTrace()
    } finally {
      if (connection != null){
        connection.close()
      }
    }
  }
}
