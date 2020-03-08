package com.hlsijx.spark.stream.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.flume.FlumeUtils

/**
  * Spark Streaming essentially sets up a receiver that acts an Avro agent for Flume,
  * to which Flume can push the data.(Not recommend by PUSH data)
  *
  * Require:
  * Choose a machine in your cluster such that
  * 1）When your Flume + Spark Streaming application is launched, one of the Spark workers must run on that machine.
  * 2）Flume can be configured to push data to a port on that machine.
  *
  * deploy:
  * 1、mvn clean deploy -DskipTests
  *
  * 2、upload target/spark-1.0.jar to `/data/spark-2.4.4-bin-2.6.0-cdh5.15.1/lib`
  *
  * 3、launch spark application
  * bin/spark-submit \
  * --class com.hlsijx.spark.stream.wordcount.FlumePushWordCount \
  * --master local[2] \
  * --packages org.apache.spark:spark-streaming-flume_2.11:2.4.4 \
  * /data/spark-2.4.4-bin-2.6.0-cdh5.15.1/lib/spark-1.0.jar hlsijx 11111
  *
  * 4、launch flume
  * flume-ng agent \
  * --conf $FLUME_HOME/conf \
  * --conf-file $FLUME_HOME/conf/streaming-flume-integration-push.conf \
  * --name integration-agent \
  * -Dflume.root.logger=INFO,console
  *
  * 5、telnet hlsijx 9999
  */
object FlumePushWordCount {

  def main(args: Array[String]): Unit = {

    if (args.length != 2){
      System.err.print("Usage: FlumePushWordCount <hostname> <port>")
      System.exit(1)
    }
    val Array(hostname, port) = args

    val sparkConf = new SparkConf()

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = FlumeUtils.createStream(ssc, hostname, port.toInt)

    val wordCount = lines.map(x => new String(x.event.getBody.array()).trim)
                    .flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    wordCount.print()

    ssc.start()

    ssc.awaitTermination()
  }
}
