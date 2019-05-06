package com.johnny.flume.sink.kafka;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class CustomKafkaSink extends AbstractSink implements Configurable {
    public KafkaProducer<String, String> producer;
    public String topic;

    @Override
    public Status process() throws EventDeliveryException {

        System.out.print("---------------------------process------------------------------------");

        Status status = null;
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        try {

            Event event = ch.take();
            if (event == null) {
                status = Status.BACKOFF;
            }
            byte[] byte_message = event.getBody();
            //生产者
            ProducerRecord<String, String> record = new ProducerRecord<>(this.topic, new String(byte_message));
            producer.send(record);

            txn.commit();
            status = Status.READY;
        } catch (Throwable t) {
            txn.rollback();
            status = Status.BACKOFF;
            if (t instanceof Error) {
                throw (Error) t;
            }
        } finally {
            txn.close();
        }
        return status;
    }

    @Override
    public void configure(Context context) {

        System.out.print("---------------------------configure------------------------------------");

        Properties properties = new Properties();
        properties.put("bootstrap.servers", context.getString("bootstrapServers"));
        properties.put("metadata.broker.list", context.getString("brokerList"));
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("request.required.acks", context.getString("acks"));
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");

        this.producer = new KafkaProducer<String, String>(properties);
        this.topic = context.getString("topic");
    }
}
