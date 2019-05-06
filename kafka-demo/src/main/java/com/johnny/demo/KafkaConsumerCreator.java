package com.johnny.demo;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerCreator extends Thread {

    public String topic;

    public KafkaConsumer<byte[], byte[]> consumer;

    public KafkaConsumerCreator(String topic) {
        this.topic = topic;

        Properties properties = new Properties();
        properties.put("zookeeper.connect", KafkaProperties.ZK);
        properties.put("group.id", KafkaProperties.GROUP_ID);

        properties.put("auto.offset.reset", "earliest");
        properties.put("bootstrap.servers", KafkaProperties.BROKER_LIST);
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        this.consumer = new KafkaConsumer<byte[], byte[]>(properties);
    }

    @Override
    public void run() {
        KafkaConsumerRebalanceListener rebalanceListener = new KafkaConsumerRebalanceListener();

        consumer.subscribe(Collections.singletonList(topic), rebalanceListener);

        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
            for (ConsumerRecord<byte[], byte[]> record : records) {
                System.out.printf("resd: %s\n", record.value());
            }

            consumer.commitSync();
        }
    }
}




