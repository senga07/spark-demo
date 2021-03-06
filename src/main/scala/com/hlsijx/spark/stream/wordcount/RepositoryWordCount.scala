package com.hlsijx.spark.stream.wordcount

import java.sql.{Connection, PreparedStatement}

import com.hlsijx.spark.CommonConfig
import com.hlsijx.spark.utils.MySqlUtils
import com.hlsijx.spark.stream.factory.SparkStreamFactory

/**
  * Save word count result to Mysql
  * 实时监听9999端口的输入内容进行词频统计，并将结果保存到MySql中
  *
  * Key Func:foreachRDD
  */
object RepositoryWordCount {

  def main(args: Array[String]): Unit = {

    val ssc = SparkStreamFactory.createStreamingContext("RepositoryWordCount")

    val lines = ssc.socketTextStream(CommonConfig.hostname, CommonConfig.netcat_port)

    val wordCount = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    wordCount.print()

    /**
      * 最优实现：
      * 数据库连接使用连接池，使用完毕后或超时将连接放回连接池
      * 关键点：要在每个partition下获取数据库连接
      */
    wordCount.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords =>
        val connection = MySqlUtils.getConnection()
        var pstmt : PreparedStatement = null
        partitionOfRecords.foreach(record => pstmt = executeSql(connection, record))
        MySqlUtils.release(connection, pstmt)
      }
    }

    SparkStreamFactory.startStreamingJob(ssc)
  }

  def executeSql(connection : Connection, item : (String, Int)): PreparedStatement ={
    val sqlQuery = "insert into word_count(wordkey,wordvalue) value (?,?)"
    val pstmt = connection.prepareStatement(sqlQuery)
    pstmt.setString(1, item._1)
    pstmt.setInt(2, item._2)

    pstmt.execute()
    pstmt
  }
}
