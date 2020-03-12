package com.hlsijx.spark.stream.kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 集成Kafka实现词频统计(Receiver-based Approach)
  *
  * 执行命令：
  * spark-submit \
  * --class com.hlsijx.spark.stream.wordcount.KafkaReceiverWordCount \
  * --master local[2] \
  * --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 \
  * /data/spark-2.4.4-bin-2.6.0-cdh5.15.1/lib/spark-1.0.jar hlsijx:2181 1 hello-spark
  *
  */
object KafkaReceiver {

  def main(args: Array[String]): Unit = {

    if (args.length != 3){
      System.err.print("Usage: KafkaReceiverWordCount <zkQuorum> <groupId> <topic>")
      System.exit(1)
    }

    val Array(zkQuorum, groupId, topic) = args

    val ssc = new StreamingContext(new SparkConf(), Seconds(5))

    val topicMap = Map(topic -> 1)
    val kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap)
    val wordCount = kafkaStream.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}