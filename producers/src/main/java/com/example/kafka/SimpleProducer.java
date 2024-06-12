package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleProducer {
    public static void main(String[] args) {

        String topicName = "test-topic";

        // kafka producer configuration

        Properties props = new Properties();
        // bootstrap.servers, key.serializer.class, value.serializer.class
//        props.setProperty("bootstrap.servers", "52.78.108.58:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "52.78.108.58:9092");
        // both are correct, but lower code is more stable when the parameter's name change
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer Object
        // <key object type, value object type>
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        // ProducerRecord Object
        // if key parameter is empty, key will be null
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "hello world2");

        // KafkaProducer message send Object
        kafkaProducer.send(producerRecord);

        // flush : to send message in the buffer
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
