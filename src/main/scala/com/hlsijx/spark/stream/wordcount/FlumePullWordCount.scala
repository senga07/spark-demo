package com.hlsijx.spark.stream.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * This ensures stronger reliability and fault-tolerance guarantees than the previous approach.
  * However, this requires configuring Flume to run a custom sink(Recommend by PULL data)
  *
  * Require:
  * Choose a machine that will run the custom sink in a Flume agent.
  * The rest of the Flume pipeline is configured to send data to that agent.
  * Machines in the Spark cluster should have access to the chosen machine running the custom sink.
  *
  * deploy:
  * 1、mvn clean deploy -DskipTests
  *
  * 2、upload target/spark-1.0.jar to `/data/spark-2.4.4-bin-2.6.0-cdh5.15.1/lib`
  *
  * 3、launch flume
  * flume-ng agent \
  * --conf $FLUME_HOME/conf \
  * --conf-file $FLUME_HOME/conf/streaming-flume-integration-pull.conf \
  * --name integration-agent \
  * -Dflume.root.logger=INFO,console
  *
  * 4、launch spark application
  * bin/spark-submit \
  * --class com.hlsijx.spark.stream.wordcount.FlumePullWordCount \
  * --master local[2] \
  * --packages org.apache.spark:spark-streaming-flume_2.11:2.4.4 \
  * /data/spark-2.4.4-bin-2.6.0-cdh5.15.1/lib/spark-1.0.jar hlsijx 11111
  *
  * 5、telnet hlsijx 9999
  */
object FlumePullWordCount {

  def main(args: Array[String]): Unit = {

    if (args.length != 2){
      System.err.print("Usage: FlumePullWordCount <hostname> <port>")
      System.exit(1)
    }
    val Array(hostname, port) = args

    val sparkConf = new SparkConf()

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = FlumeUtils.createPollingStream(ssc, hostname, port.toInt)

    val wordCount = lines.map(x => new String(x.event.getBody.array()).trim)
                    .flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    wordCount.print()

    ssc.start()

    ssc.awaitTermination()
  }
}
