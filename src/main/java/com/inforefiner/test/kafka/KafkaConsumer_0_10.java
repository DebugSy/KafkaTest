package com.inforefiner.test.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Properties;


/**
 * Created by DebugSy on 2017/7/31.
 */
@Slf4j
public class KafkaConsumer_0_10 {

    public static void main(String[] args) {

        String brokerList = args[0];
        String topic = args[1];

        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                consumer.seekToBeginning(partitions);
            }
        });//订阅topic

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                log.info("offset = {}, partition={}, timestamp={}, timestampType={}, key = {}, value = {}",
                        record.offset(), record.partition(), dateFormat.format(new Date(record.timestamp())), record.timestampType(), record.key(), record.value());
                split(record.value());
            }
        }

    }

    public static void split(String originMessage) {
        String headSeparator = "@";
        String messageSeparator = "\\n";
        String separator = "[|]";
        boolean contained = originMessage.contains(headSeparator);
        if (contained) {
            int headSepIndex = originMessage.indexOf(headSeparator);
            originMessage = originMessage.substring(headSepIndex + 1);
        }

        String[] messages = originMessage.split(messageSeparator);
        for (int i = 0; i < messages.length; i++) {
            String message = messages[i];
            log.info("message{}: {}", i, message);
            String[] columns = message.split(separator);
            for (int j = 0; j < columns.length; j++) {
                String column = columns[j];
                log.info("column{}: {}", j, column);
            }
        }
    }


}
