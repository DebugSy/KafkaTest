package com.inforefiner.test.kafka;

import com.google.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * Created by P0007 on 2020/03/09.
 */
@Slf4j
public class KafkaProducer_0_10 implements Runnable{

    private final int partition;

    private final long batchSize;
    private final long limit;

    private static final String SEPARATOR = ",";

    private String bootstrap;

    private String topic;

    private RateLimiter rateLimiter;

    public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");

    public KafkaProducer_0_10(int partition, String bootstrap, String topic, long batchSize, long limit, double rate) {
        this.partition = partition;
        this.bootstrap = bootstrap;
        this.topic = topic;
        this.batchSize = batchSize;
        this.limit = limit;
        this.rateLimiter = RateLimiter.create(rate);
    }

    /**
     * bootstrap - kafka broker server地址
     * topic - kafka topic
     * partitionSize - topic有多少个分区，同时开启这么多个线程发送消息
     * batchSize - 每隔多少条数据打印一次log
     * limit - 发送的kafka message总条数限制，超过此条数，程序退出
     * rate - 发送kafka message的速率 100.0代表每秒100条
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args) {
        String bootstrap = args[0];
        String topic = args[1];
        int partitionSize = Integer.valueOf(args[2]);
        long batchSize = Long.valueOf(args[3]);
        long limit = Long.valueOf(args[4]);
        double rate = Double.valueOf(args[5]);
        for (int i = 0; i < partitionSize; i++) {
            KafkaProducer_0_10 kafkaProducer = new KafkaProducer_0_10(i, bootstrap, topic, batchSize, limit, rate);
            Thread thread = new Thread(kafkaProducer);
            thread.setName("Thread-" + i);
            thread.start();
            log.info("Starting thread, name: " + thread.getName());
        }
    }

    @Override
    public void run() {
        KafkaProducer kafkaProducer = initProducer();
        produce(kafkaProducer, topic, limit);
    }

    public KafkaProducer initProducer() {
        Properties props = new Properties();
        InputStream inputStream = this.getClass().getResourceAsStream("/kafka-producer.properties");
        try {
            props.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        props.put("bootstrap.servers", bootstrap);
//        props.put("acks", "0");
//        props.put("retries", "3");
//        props.put("batch.size", "729444");
//        props.put("buffer.memory", "33554432");
//        props.put("request.timeout.ms", "120000");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);
        return kafkaProducer;
    }

    public void produce(KafkaProducer producer, String topic, long limit) {
        int count = 0;
        while (true) {
            rateLimiter.acquire();
            StringBuilder message = generateUrlClickMessage();;
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, partition, null, message.toString());
            Callback callback = (metadata, e) -> {
                if (e != null) {
                    log.error("Error while sending record to Kafka: " + e.getMessage(), e);
                }
            };
            producer.send(record, callback);
            count++;
            if (count % batchSize == 0) {
                log.info("count {}, {}", count, message.toString());
            }
            if (count == limit) {
                break;
            }
        }
    }

    private StringBuilder generateUrlClickMessage() {
        Random random = new Random(System.currentTimeMillis());
        String col1  = dateFormat.format(new Date(System.currentTimeMillis()));
        String col2  = String.valueOf(random.nextInt(9999));
        String col3  = String.valueOf(random.nextInt(143593810));
        String col4  = UUID.randomUUID().toString().substring(0, 16);
        String col5  = String.valueOf(random.nextInt(100));
        String col6  = String.valueOf(random.nextInt(1000));
        String col7  = String.valueOf(random.nextInt(20));
        String col8  = String.valueOf(random.nextInt(100));
        String col9  = String.valueOf(random.nextInt(10));
        String col10 = String.valueOf(random.nextInt(10));
        String col11 = String.valueOf(random.nextInt(10));
        String col12 = String.valueOf(random.nextInt(10));
        String col13 = String.valueOf(random.nextInt(10));
        String col14 = String.valueOf(random.nextInt(10));
        String col15 = String.valueOf(random.nextInt(10));
        String col16 = String.valueOf(random.nextInt(10));
        String col17 = String.valueOf(random.nextInt(10));
        String col18 = String.valueOf(random.nextInt(10));
        String col19 = String.valueOf(random.nextInt(10));
        String col20 = String.valueOf(random.nextInt(10));
        String col21 = String.valueOf(random.nextInt(10));
        String col22 = String.valueOf(random.nextInt(10));
        String col23 = String.valueOf(random.nextInt(10));
        String col24 = String.valueOf(random.nextInt(10));
        String col25 = String.valueOf(random.nextInt(10));
        String col26 = String.valueOf(random.nextInt(10));
        String col27 = String.valueOf(random.nextInt(10));
        String col28 = String.valueOf(random.nextInt(10));
        String col29 = String.valueOf(random.nextInt(10));
        String col30 = String.valueOf(random.nextInt(10));
        String col31 = String.valueOf(random.nextInt(10));
        String col32 = String.valueOf(random.nextInt(10));
        String col33 = String.valueOf(random.nextInt(10));
        String col34 = String.valueOf(random.nextInt(10));
        String col35 = String.valueOf(random.nextInt(10));
        String col36 = String.valueOf(random.nextInt(10));
        String col37 = String.valueOf(random.nextInt(10));
        String col38 = String.valueOf(random.nextInt(10));
        String col39 = String.valueOf(random.nextInt(10));
        String col40 = String.valueOf(random.nextInt(10));
        String col41 = String.valueOf(random.nextInt(10));
        String col42 = String.valueOf(random.nextInt(10));
        return new StringBuilder()
                .append(col1)
                .append(SEPARATOR).append(col2)
                .append(SEPARATOR).append(col3)
                .append(SEPARATOR).append(col4)
                .append(SEPARATOR).append(col5)
                .append(SEPARATOR).append(col6)
                .append(SEPARATOR).append(col7)
                .append(SEPARATOR).append(col8)
                .append(SEPARATOR).append(col9)
                .append(SEPARATOR).append(col10)
                .append(SEPARATOR).append(col11)
                .append(SEPARATOR).append(col12)
                .append(SEPARATOR).append(col13)
                .append(SEPARATOR).append(col14)
                .append(SEPARATOR).append(col15)
                .append(SEPARATOR).append(col16)
                .append(SEPARATOR).append(col17)
                .append(SEPARATOR).append(col18)
                .append(SEPARATOR).append(col19)
                .append(SEPARATOR).append(col20)
                .append(SEPARATOR).append(col21)
                .append(SEPARATOR).append(col22)
                .append(SEPARATOR).append(col23)
                .append(SEPARATOR).append(col24)
                .append(SEPARATOR).append(col25)
                .append(SEPARATOR).append(col26)
                .append(SEPARATOR).append(col27)
                .append(SEPARATOR).append(col28)
                .append(SEPARATOR).append(col29)
                .append(SEPARATOR).append(col30)
                .append(SEPARATOR).append(col31)
                .append(SEPARATOR).append(col32)
                .append(SEPARATOR).append(col33)
                .append(SEPARATOR).append(col34)
                .append(SEPARATOR).append(col35)
                .append(SEPARATOR).append(col36)
                .append(SEPARATOR).append(col37)
                .append(SEPARATOR).append(col38)
                .append(SEPARATOR).append(col39)
                .append(SEPARATOR).append(col40)
                .append(SEPARATOR).append(col41)
                .append(SEPARATOR).append(col42);
    }

}
