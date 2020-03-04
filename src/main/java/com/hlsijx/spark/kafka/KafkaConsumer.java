package com.hlsijx.spark.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.hlsijx.spark.kafka.constants.CustomProperties.*;
import static com.hlsijx.spark.kafka.constants.KafkaProperties.*;

/**
 * kafka消费者
 */
public class KafkaConsumer extends Thread{

    private String topic;

    public KafkaConsumer(String topic){
        this.topic = topic;
    }

    private ConsumerConnector consumerConnector(){

        Properties properties = new Properties();
        properties.setProperty(ZOOKEEPER_CONNECT, ZOOKEEPER_URL);
        properties.setProperty(GROUP_ID, GROUP_ID_VALUE);
        properties.setProperty(ADVERTISED_HOST_NAME, HOSTNAME);
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }

    @Override
    public void run() {

        Map<String, Integer> topicCountMap = new HashMap<>(2);
        topicCountMap.put(topic, 1);
        ConsumerConnector consumerConnector = consumerConnector();
        Map<String, List<KafkaStream<byte[], byte[]>>> message = consumerConnector.createMessageStreams(topicCountMap);

        KafkaStream<byte[], byte[]> kafkaStream = message.get(topic).get(0);
        for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : kafkaStream) {
            String msg = new String(messageAndMetadata.message());
            System.out.println("=========rec" + msg);
        }
    }
}
