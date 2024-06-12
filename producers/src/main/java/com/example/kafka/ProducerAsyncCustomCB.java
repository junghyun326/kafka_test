package com.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerAsyncCustomCB {
    // import Logger for printing details
    public static final Logger logger = LoggerFactory.getLogger(ProducerAsyncCustomCB.class.getName());
    public static void main(String[] args) {

        String topicName = "multipart-topic";

        // kafka producer configuration

        Properties props = new Properties();
        // bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty("bootstrap.servers", "13.124.181.252:9092");
    //        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "13.124.135.164:9092");
        // both are correct, but lower code is more stable when the parameter's name change
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer Object
        // <key object type, value object type>
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<Integer, String>(props);

        for(int seq = 0; seq < 20; seq++) {

            // ProducerRecord Object
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topicName, seq, "hello world " + seq);
            Callback callback = new CustomCallback(seq);

            // logger.info("seq:" + seq);
            // KafkaProducer message send Object with async mode
            kafkaProducer.send(producerRecord, callback);
        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();

    }
}
