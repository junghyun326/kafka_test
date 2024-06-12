package com.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerMTopicRebalance {
    public static final Logger logger = LoggerFactory.getLogger(ConsumerMTopicRebalance.class.getName());
    public static void main(String[] args) {

//        String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "43.203.123.63:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-mtopic");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-assign");
        // default : props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
//        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        props.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "3");
        // waiting 60 seconds
//        props.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");
        // rebalancing strategy
        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
        // can listen several topics
        // kafkaConsumer.subscribe(List.of("simple-topic", "multipart-topic"));
        kafkaConsumer.subscribe(List.of("topic-p3-t1", "topic-p3-t2"));

        // main thread
        Thread mainThread = Thread.currentThread();

        // make dead letter
        // To avoid shutting down main thread, use shutdown hook executing code during shutting down application.
        // (another thread, wake up method thread in the sentence.)
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("Main program starts to exit by calling wakeup");
                kafkaConsumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        int loopCnt = 0;

        try { // To close consumer, use try/catch and wakeupException
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info(" ######## loopCnt: {} consumerRecords count: {}", loopCnt++, consumerRecords.count());
                for (ConsumerRecord record : consumerRecords) {
                    logger.info("topic:{}, record.key:{}, record.value:{}, record.partition:{}, record.offset:{}",
                            record.topic(), record.key(), record.value(), record.partition(), record.offset());
                }
                try {
                    logger.info("main thread is sleeping {} ms during while loop", loopCnt*10000);
                    Thread.sleep(loopCnt*10000);
                } catch(InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch(WakeupException e) {
            logger.error("Wakeup exception has been called");
        } finally {
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }

    }
}
