package org.bigdatacenter.kafka;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.bigdatacenter.kafka.consumer.KafkaExampleConsumer;
import org.bigdatacenter.kafka.producer.KafkaExampleProducer;

/**
 * @author Anthony Jinhyuk Kim
 */
public class KafkaConnectionExample {
    private static final Logger logger = Logger.getLogger(KafkaConnectionExample.class);

    public static void main(String[] args) throws InterruptedException {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);

        final String topicName = "test";
        final String message = "crawled document";

        Thread producerThread = new Thread(new KafkaExampleProducer(topicName, message));
        producerThread.start();
        logger.info("The producer thread has been started.");

        Thread.sleep(1000L);

        Thread consumerThread = new Thread(new KafkaExampleConsumer(topicName));
        consumerThread.start();
        logger.info("The consumer thread has been started.");

        Thread.sleep(5000L);
    }
}
