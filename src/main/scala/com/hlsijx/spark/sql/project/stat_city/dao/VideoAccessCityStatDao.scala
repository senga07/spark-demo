package com.hlsijx.spark.sql.project.stat_city.dao

import java.sql.{Connection, PreparedStatement}

import com.hlsijx.spark.sql.project.delete.DeleteStatData
import com.hlsijx.spark.sql.project.stat_city.model.VideoAccessCityStat
import com.hlsijx.spark.sql.project.stat_video.model.VideoAccessStat
import com.hlsijx.spark.utils.MySqlUtils
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer


object VideoAccessCityStatDao {

  def addVideoAccessCityStat(result : DataFrame): Unit ={

    DeleteStatData.delete("video_access_city_stat")

    result.foreachPartition(row => {

      val list = new ListBuffer[VideoAccessCityStat]

      //将DataFrame转换为自定义的case class
      row.foreach(fields => {
        val cmsId = fields.getAs[Long]("cmsId")
        val date = fields.getAs[String]("date")
        val total = fields.getAs[Long]("total")
        val city = fields.getAs[String]("city")
        val rank = fields.getAs[Int]("rank")
        list.append(VideoAccessCityStat(cmsId, date, total, city, rank))
      })

      //将结果插入到数据库
      insert(list)
    })
  }

  private def insert(list : ListBuffer[VideoAccessCityStat]): Unit = {
    var connection : Connection = null
    var pstmt : PreparedStatement = null

    try{
      connection = MySqlUtils.getConnection()
      //关闭自动提交事务
      connection.setAutoCommit(false)

      pstmt = connection.prepareStatement("insert into video_access_city_stat(cmsid,date,total,city,rank) value (?,?,?,?,?)")
      list.foreach(item => {
        pstmt.setLong(1, item.cmsId)
        pstmt.setString(2, item.date)
        pstmt.setLong(3, item.total)
        pstmt.setString(4, item.city)
        pstmt.setInt(5, item.rank)
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
