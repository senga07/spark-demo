package com.hlsijx.spark.sql.project.stat_video.dao

import java.sql.{Connection, PreparedStatement}

import com.hlsijx.spark.sql.project.delete.DeleteStatData
import com.hlsijx.spark.sql.project.stat_video.model.VideoAccessStat
import com.hlsijx.spark.sql.utils.MySqlUtils
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer


object VideoAccessStatDao {

  def addVideoAccessStat(result : DataFrame): Unit ={

    result.foreachPartition(row => {

      DeleteStatData.delete("video_access_stat")
      val list = new ListBuffer[VideoAccessStat]

      //将DataFrame转换为自定义的case class
      row.foreach(fields => {
        val cmsId = fields.getAs[Long]("cmsId")
        val date = fields.getAs[String]("date")
        val total = fields.getAs[Long]("total")
        list.append(VideoAccessStat(cmsId, date, total))
      })

      //将结果插入到数据库
      insert(list)
    })
  }

  private def insert(list : ListBuffer[VideoAccessStat]): Unit = {
    var connection : Connection = null
    var pstmt : PreparedStatement = null

    try{
      connection = MySqlUtils.getConnection()
      //关闭自动提交事务
      connection.setAutoCommit(false)

      pstmt = connection.prepareStatement("insert into video_access_stat(cmsid,date,total) value (?,?,?)")
      list.foreach(item => {
        pstmt.setLong(1, item.cmsId)
        pstmt.setString(2, item.date)
        pstmt.setLong(3, item.total)
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
