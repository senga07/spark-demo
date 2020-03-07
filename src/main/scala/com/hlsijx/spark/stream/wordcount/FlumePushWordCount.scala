package com.hlsijx.spark.stream.wordcount

import com.hlsijx.spark.CommonConfig
import com.hlsijx.spark.stream.factory.SparkStreamFactory
import org.apache.spark.streaming.flume.FlumeUtils

/**
  * Spark Streaming essentially sets up a receiver that acts an Avro agent for Flume,
  * to which Flume can push the data.
  *
  * deploy:
  * 1、mvn clean deploy -DskipTests
  *
  * 2、upload target/spark-1.0.jar to `/data/spark-2.4.4-bin-2.6.0-cdh5.15.1/lib`
  *
  * 3、flume-ng agent \
  * --conf $FLUME_HOME/conf \
  * --conf-file $FLUME_HOME/conf/streaming-flume-integration.conf \
  * --name streaming-flume-integration \
  * -Dflume.root.logger=INFO,console
  *
  * 4、bin/spark-submit \
  * --class com.hlsijx.spark.stream.wordcount.FlumePushWordCount \
  * --master local[2] \
  * --packages org.apache.spark:spark-streaming-flume_2.11:2.4.4 \
  * /data/spark-2.4.4-bin-2.6.0-cdh5.15.1/lib/spark-1.0.jar
  */
object FlumePushWordCount {

  def main(args: Array[String]): Unit = {

    val ssc = SparkStreamFactory.createStreamingContext("PushBasedApproach")

    val lines = FlumeUtils.createStream(ssc, CommonConfig.hostname, CommonConfig.avro_port)

    val wordCount = lines.map(x => new String(x.event.getBody.array()).trim)
                    .flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    wordCount.print()

    SparkStreamFactory.startStreamingJob(ssc)
  }
}
