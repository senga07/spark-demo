package com.hlsijx.spark.sql.project.delete

import java.sql.{Connection, PreparedStatement}

import com.hlsijx.spark.utils.MySqlUtils


object DeleteStatData {

  def delete(tableName : String): Unit ={
    var connection : Connection = null
    var pstmt : PreparedStatement = null
    try {
      connection = MySqlUtils.getConnection()
      pstmt = connection.prepareStatement(s"delete from $tableName")
      pstmt.executeUpdate()
    } catch {
      case e : Exception => e.printStackTrace()
    } finally {
      MySqlUtils.release(connection, pstmt)
    }
  }
}
