package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducerAsync {
    // import Logger for printing details
    public static final Logger logger = LoggerFactory.getLogger(SimpleProducerAsync.class.getName());
    public static void main(String[] args) {

        String topicName = "simple-topic";

        // kafka producer configuration

        Properties props = new Properties();
        // bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty("bootstrap.servers", "3.38.179.10:9092");
    //        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "13.124.135.164:9092");
        // both are correct, but lower code is more stable when the parameter's name change
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer Object
        // <key object type, value object type>
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        // ProducerRecord Object
        // if key parameter is empty, key will be null
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "hello world 3-1");

        // KafkaProducer message send Object with async mode
        kafkaProducer.send(producerRecord, (metadata, exception)-> {
            if (exception == null) {
                logger.info("\n ##### record metadata received $$$$ \n" +
                        "partition:" + metadata.partition() + "\n" +
                        "offset:" + metadata.offset() + "\n" +
                        "timestamp:" + metadata.timestamp());
            } else {
                logger.error("exception error from broker " + exception.getMessage());
            }
        });

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();

    }
}
