package com.johnny.demo;


import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author JohnnyLiu
 */
public class KafkaProducerCreator extends Thread {

    public String topic;

    public KafkaProducer<String, String> producer;

    public KafkaProducerCreator(String topic) {
        this.topic = topic;

        Properties properties = new Properties();
        properties.put("bootstrap.servers", KafkaProperties.BROKER_LIST);
        properties.put("metadata.broker.list", KafkaProperties.BROKER_LIST);
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("request.required.acks", "1");
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");

        producer = new KafkaProducer<String, String>(properties);
    }

    @Override
    public void run() {
        int index = 100;

        while (true) {

            KafkaProducerCallback kpc = new KafkaProducerCallback();

            String sendMessage = "message_" + index;

            ProducerRecord<String, String> km = new ProducerRecord<String, String>(topic, sendMessage);
            producer.send(km, kpc);

            System.out.println("sent: " + sendMessage);

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            index++;
        }
    }

}
