package org.bigdatacenter.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * @author Anthony Jinhyuk Kim
 */
public class KafkaExampleProducer implements Runnable {
    private static final Logger logger = Logger.getLogger(KafkaExampleProducer.class);
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private String topicName;
    private String message;

    public KafkaExampleProducer(String topicName, String message) {
        this.topicName = topicName;
        this.message = message;
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<Long, String> kafkaProducer = new KafkaProducer<>(props);
        kafkaProducer.send(new ProducerRecord<>(topicName, System.currentTimeMillis(), message));
        kafkaProducer.close();
    }
}