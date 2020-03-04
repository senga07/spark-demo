package com.hlsijx.spark.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

import static com.hlsijx.spark.kafka.constants.CustomProperties.*;
import static com.hlsijx.spark.kafka.constants.KafkaProperties.*;

/**
 * kafka生产者
 */
public class KafkaProducer extends Thread{

    private String topic;

    private Producer<Integer, String> producer;

    public KafkaProducer(String topic){

        this.topic = topic;

        Properties properties = new Properties();
        properties.setProperty(METADATE_BROKER_LIST, BROKER_LIST);
        properties.setProperty(SERIALIZER_CLASS, STRING_ENCODER);
        properties.setProperty(REQUEST_REQUIRED_ACKS, "1");
        properties.setProperty(ADVERTISED_HOST_NAME, HOSTNAME);
        producer = new Producer<>(new ProducerConfig(properties));
    }

    @Override
    public void run() {

        int id = 0;
        String msg;

        while (true){
            msg = "message_" + id;
            producer.send(new KeyedMessage<>(topic, msg));
            System.out.println("－－－－－－" +msg);
            id ++;

            if (id > 10 ){
                break;
            }
            try {
                sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
