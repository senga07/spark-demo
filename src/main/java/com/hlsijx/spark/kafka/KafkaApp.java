package com.hlsijx.spark.kafka;

import static com.hlsijx.spark.kafka.constants.CustomProperties.TOPIC;

/**
 * 测试
 */
public class KafkaApp {

    public static void main(String[] args) {

        new KafkaProducer(TOPIC).start();

        new KafkaConsumer(TOPIC).start();
    }
}
