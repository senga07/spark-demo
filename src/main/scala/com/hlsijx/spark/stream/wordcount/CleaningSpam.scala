package com.hlsijx.spark.stream.wordcount

import com.hlsijx.spark.CommonConfig
import com.hlsijx.spark.stream.factory.SparkStreamFactory

/**
  * 实时拦截垃圾邮件
  * RDD与DStream的交互操作
  * The transform operation allows arbitrary RDD-to-RDD functions to be applied on a DStream
  *
  * Key Fun:transform
  */
object CleaningSpam {

  def main(args: Array[String]): Unit = {

    val ssc = SparkStreamFactory.createStreamingContext("CleaningSpam")

    /**
      * 通常垃圾邮件名单配置在数据库中
      * spamInfoRDD转换后的数据格式
      * (111@qq.com, true),
      * (222@qq.com, true),
      * (333@qq.com, true)
      */
    val spamInfoRDD = ssc.sparkContext.textFile("./test-data/txt/spam.txt").map(x => (x, true))

    /**
      * 输入格式
      * 20200111 111@qq.com
      * 20200111 222@qq.com
      * 20200111 444@qq.com
      */
    val lines = ssc.socketTextStream(CommonConfig.hostname, CommonConfig.port)

    /**
      * email格式
      * (111@qq.com, 20200111 111@qq.com),
      * (222@qq.com, 20200111 222@qq.com),
      * (444@qq.com, 20200111 444@qq.com),
      */
    val email = lines.map(x => (x.split(" ")(1), x))

    /**
      * afterLeftJoin格式
      * (111@qq.com,(20200111 111@qq.com, true)),
      * (222@qq.com,(20200111 222@qq.com, true)),
      * (444@qq.com,(20200111 444@qq.com, false))
      */
    val cleanedDStream = email.transform { rdd =>
      rdd.leftOuterJoin(spamInfoRDD)
        .filter(x => !x._2._2.getOrElse(false))
        .map(x => x._2._1)
    }

    cleanedDStream.print()

    SparkStreamFactory.startStreamingJob(ssc)
  }
}
