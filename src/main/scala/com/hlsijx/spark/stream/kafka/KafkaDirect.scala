package com.hlsijx.spark.stream.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}


object KafkaDirect {

  def main(args: Array[String]): Unit = {

    if (args.length != 2){
      System.err.print("Usage: KafkaDirect <brokerList> <topic>")
      System.exit(1)
    }

    val Array(brokerList, topic) = args

    val sparkConf = new SparkConf().setAppName("KafkaDirect").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaParams = Map("metadata.broker.list" -> brokerList)
    val topics = topic.split(",").toSet
    val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val wordCount = directKafkaStream.map(_._2)

    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}