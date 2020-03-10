package com.hlsijx.spark;

import org.apache.log4j.Logger;

/**
 * 日志生成器，用于测试
 */
public class LoggerGenerator {

    private static Logger logger = Logger.getLogger(LoggerGenerator.class);

    public static void main(String[] args) throws InterruptedException {
        int i = 0;
        while (true){
            logger.info("value:" + (i++));
            Thread.sleep(1000);
        }
    }
}
