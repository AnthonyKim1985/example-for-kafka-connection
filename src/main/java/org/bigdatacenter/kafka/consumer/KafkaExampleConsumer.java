package org.bigdatacenter.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Properties;

/**
 * @author Anthony Jinhyuk Kim
 */
public class KafkaExampleConsumer implements Runnable {
    private static final Logger logger = Logger.getLogger(KafkaExampleConsumer.class);
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private String topicName;

    public KafkaExampleConsumer(String topicName) {
        this.topicName = topicName;
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        final Consumer<Long, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));

        final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000L);
        consumerRecords.forEach(record -> logger.info(String.format("Consumer Record:(%d, %s, %d, %d)\n", record.key(), record.value(), record.partition(), record.offset())));

        consumer.commitAsync();
        consumer.close();
    }
}
