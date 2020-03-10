package com.hlsijx.spark.stream.wordcount

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._

/**
  * 集成Kafka实现词频统计(Direct Approach)
  *
  * 执行命令：
  * spark-submit \
  * --class com.hlsijx.spark.stream.wordcount.KafkaDirectWordCount \
  * --master local[2] \
  * --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 \
  * /data/spark-2.4.4-bin-2.6.0-cdh5.15.1/lib/spark-1.0.jar hlsijx:9092 hello-spark
  *
  */
object KafkaDirectWordCount {

  def main(args: Array[String]): Unit = {

    if (args.length != 2){
      System.err.print("Usage: KafkaDirectWordCount <brokerList> <topic>")
      System.exit(1)
    }

    val Array(brokerList, topic) = args

    val ssc = new StreamingContext(new SparkConf(), Seconds(5))

    val kafkaParams = Map("metadata.broker.list" -> brokerList)
    val topics = topic.split(",").toSet
    val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val wordCount = directKafkaStream.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}