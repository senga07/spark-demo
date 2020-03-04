package com.hlsijx.spark.kafka.constants;

/**
 * Kafka属性
 */
public class CustomProperties {


    /**
     * 服务器ip
     */
    public static final String HOSTNAME = "hlsijx";

    /**
     * zookeeper端口号
     */
    private static final String ZOOKEEPER_PORT = "2181";

    /**
     * kafka broker的端口号
     */
    private static final String SERVER_PORT = "9092";

    /**
     * zookeeper的连接地址
     */
    public static final String ZOOKEEPER_URL = HOSTNAME + ":" + ZOOKEEPER_PORT;

    /**
     * brokerlist的连接地址
     */
    public static final String BROKER_LIST = HOSTNAME + ":" + SERVER_PORT;

    /**
     * topic名称
     */
    public static final String TOPIC = "hello-spark";

    public static final String GROUP_ID_VALUE= "spark_group";
}
