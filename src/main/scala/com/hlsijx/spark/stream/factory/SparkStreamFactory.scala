package com.hlsijx.spark.stream.factory

import com.hlsijx.spark.WindowsEnv
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreamFactory {

  /**
    * Create StreamingContext
    * master:local[2]
    * time inverval:5s
    */
  def createStreamingContext(appName : String): StreamingContext ={

    WindowsEnv.setWinEnv()

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName(appName)

    new StreamingContext(sparkConf, Seconds(5))
  }

  /**
    * Strat a streaming job until finish
    */
  def startStreamingJob(ssc : StreamingContext): Unit ={

    ssc.start()

    ssc.awaitTermination()
  }
}
