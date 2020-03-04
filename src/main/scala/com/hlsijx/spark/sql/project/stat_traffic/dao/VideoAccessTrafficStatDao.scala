package com.hlsijx.spark.sql.project.stat_traffic.dao

import java.sql.{Connection, PreparedStatement}

import com.hlsijx.spark.sql.project.delete.DeleteStatData
import com.hlsijx.spark.sql.project.stat_traffic.model.VideoAccessTrafficStat
import com.hlsijx.spark.sql.utils.MySqlUtils
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer


object VideoAccessTrafficStatDao {

  def addVideoAccessTrafficStat(result : DataFrame): Unit ={

    DeleteStatData.delete("video_access_traffic_stat")
    result.show(10, truncate = false)

    result.foreachPartition(row => {

      val list = new ListBuffer[VideoAccessTrafficStat]

      //将DataFrame转换为自定义的case class
      row.foreach(fields => {
        val cmsId = fields.getAs[Long]("cmsId")
        val date = fields.getAs[String]("date")
        val trafficSum = fields.getAs[Long]("traffic_sum")
        list.append(VideoAccessTrafficStat(cmsId, date, trafficSum))
      })

      //将结果插入到数据库
      insert(list)
    })
  }

  private def insert(list : ListBuffer[VideoAccessTrafficStat]): Unit = {
    var connection : Connection = null
    var pstmt : PreparedStatement = null

    try{
      connection = MySqlUtils.getConnection()
      //关闭自动提交事务
      connection.setAutoCommit(false)

      pstmt = connection.prepareStatement("insert into video_access_traffic_stat(cmsid,date,traffic_sum) value (?,?,?)")
      list.foreach(item => {
        pstmt.setLong(1, item.cmsId)
        pstmt.setString(2, item.date)
        pstmt.setLong(3, item.trafficSum)
        pstmt.addBatch()
      })

      //批量插入，提高性能
      pstmt.executeBatch()
      //提交事务
      connection.commit()
    } catch {
      case e : Exception => e.printStackTrace()
    } finally {
      MySqlUtils.release(connection, pstmt)
    }
  }
}
