package com.johnny.flume.source.kafka;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.nio.charset.Charset;
import java.util.*;

public class CustomKafkaSource extends AbstractSource implements Configurable, PollableSource {

    private KafkaConsumer<byte[], String> consumer;

    @Override
    public Status process() throws EventDeliveryException {

        List<Event> eventList = new ArrayList<Event>();

        try {
            ConsumerRecords<byte[], String> records = consumer.poll(1000);

            Event event;
            Map<String, String> header;

            for (ConsumerRecord<byte[], String> record : records) {
                header = new HashMap<String, String>();
                header.put("timestamp", String.valueOf(System.currentTimeMillis()));

                event = EventBuilder.withBody(record.value(), Charset.forName("UTF-8"), header);
                eventList.add(event);
            }

        } catch (Exception e) {
            e.printStackTrace();
            return Status.BACKOFF;
        }

        getChannelProcessor().processEventBatch(eventList);

        return Status.READY;
    }

    @Override
    public void configure(Context context) {

        Properties properties = new Properties();
        properties.put("zookeeper.connect", context.getString("zk"));
        properties.put("group.id", context.getString("groupId"));
        properties.put("auto.offset.reset", "earliest");
        properties.put("bootstrap.servers", context.getString("bootstrapServers"));
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        this.consumer = new KafkaConsumer<byte[], String>(properties);
        this.consumer.subscribe(Arrays.asList(context.getString("topics").split(",")));
    }

    @Override
    public synchronized void stop() {
        super.stop();
    }
}
