package com.practice.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class FileToDBConsumer<K extends Serializable, V extends Serializable> {
    public static final Logger logger = LoggerFactory.getLogger(FileToDBConsumer.class.getName());
    protected KafkaConsumer<K, V> kafkaConsumer;
    protected List<String> topics;

    private OrderDBHandler orderDBHandler;
    public FileToDBConsumer(Properties consumerProps, List<String> topics,
                            OrderDBHandler orderDBHandler) {
        this.kafkaConsumer = new KafkaConsumer<K, V>(consumerProps);
        this.topics = topics;
        this.orderDBHandler = orderDBHandler;
    }
    public void initConsumer() {
        this.kafkaConsumer.subscribe(this.topics);
        shutdownHookToRuntime(this.kafkaConsumer);
    }

    private void shutdownHookToRuntime(KafkaConsumer<K, V> kafkaConsumer) {
        //main thread
        Thread mainThread = Thread.currentThread();

        //when main thread is shut down, another thread call KafkaConsumer wakeup() method.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info(" main program starts to exit by calling wakeup");
                kafkaConsumer.wakeup();

                try {
                    mainThread.join();
                } catch(InterruptedException e) { e.printStackTrace();}
            }
        });

    }

    private void processRecord(ConsumerRecord<K, V> record) throws Exception {
        // insert data to DB
        OrderDTO orderDTO = makeOrderDTO(record);
        this.orderDBHandler.insertOrder(orderDTO);
    }

    private OrderDTO makeOrderDTO(ConsumerRecord<K,V> record) throws Exception {
        // change data format
        String messageValue = (String)record.value();
        logger.info("##### messageValue:" + messageValue);
        String[] tokens = messageValue.split(",");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        OrderDTO orderDTO = new OrderDTO(tokens[0], tokens[1],tokens[2],tokens[3],tokens[4],tokens[5], LocalDateTime.parse(tokens[6].trim(), formatter));

        return orderDTO;
    }


    private void processRecords(ConsumerRecords<K, V> records) throws Exception{
        // insert data to DB
        List<OrderDTO> orders = makeOrders(records);
        this.orderDBHandler.insertOrders(orders);
    }

    private List<OrderDTO> makeOrders(ConsumerRecords<K,V> records) throws Exception {
        // make order from record one by one
        List<OrderDTO> orders = new ArrayList<>();
        for(ConsumerRecord<K, V> record : records) {
            OrderDTO orderDTO = makeOrderDTO(record);
            orders.add(orderDTO);
        }
        return orders;
    }


    public void pollConsumes(long durationMillis, String commitMode) {
        if (commitMode.equals("sync")) {
            pollCommitSync(durationMillis);
        } else {
            pollCommitAsync(durationMillis);
        }
    }
    private void pollCommitAsync(long durationMillis) {
        try {
            while (true) {
                ConsumerRecords<K, V> consumerRecords = this.kafkaConsumer.poll(Duration.ofMillis(durationMillis));
                logger.info("consumerRecords count:" + consumerRecords.count());
                if(consumerRecords.count() > 0) {
                    // if there is record, execute commit
                    // and even if error, do not close.
                    try {
                        processRecords(consumerRecords);
                    } catch(Exception e) {
                        logger.error(e.getMessage());
                    }
                }
//                if(consumerRecords.count() > 0) {
//                    for (ConsumerRecord<K, V> consumerRecord : consumerRecords) {
//                        processRecord(consumerRecord);
//                    }
//                }
                // change OffsetCommitCallback of commitAsync to lambda format.
                // lower sentence is more efficient than upper sentence.
                this.kafkaConsumer.commitAsync((offsets, exception) -> {
                    if (exception != null) {
                        logger.error("offsets {} is not completed, error:{}", offsets, exception.getMessage());
                    }
                });
            }
        }catch(WakeupException e) {
            logger.error("wakeup exception has been called");
        }catch(Exception e) {
            logger.error(e.getMessage());
        }finally {
            logger.info("##### commit sync before closing");
            kafkaConsumer.commitSync();
            logger.info("finally consumer is closing");
            close();
        }
    }

    protected void pollCommitSync(long durationMillis) {
        try {
            while (true) {
                ConsumerRecords<K, V> consumerRecords = this.kafkaConsumer.poll(Duration.ofMillis(durationMillis));
                processRecords(consumerRecords);
                try {
                    // if there is record, execute commit
                    // and if error, throw exception and get out from while.
                    if (consumerRecords.count() > 0) {
                        this.kafkaConsumer.commitSync();
                        logger.info("commit sync has been called");
                    }
                } catch (CommitFailedException e) {
                    logger.error(e.getMessage());
                }
            }
        }catch(WakeupException e) {
            logger.error("wakeup exception has been called");
        }catch(Exception e) {
            logger.error(e.getMessage());
        }finally {
            logger.info("##### commit sync before closing");
            kafkaConsumer.commitSync();
            logger.info("finally consumer is closing");
            close();
        }
    }
    protected void close() {
        this.kafkaConsumer.close();
        this.orderDBHandler.close();
    }

    public static void main(String[] args) {
        String topicName = "file-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "3.34.141.239:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "file-group");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        String url = "jdbc:postgresql://3.34.141.239:5432/postgres";
        String user = "postgres";
        String password = "postgres";
        OrderDBHandler orderDBHandler = new OrderDBHandler(url, user, password);

        FileToDBConsumer<String, String> fileToDBConsumer = new
                FileToDBConsumer<String, String>(props, List.of(topicName), orderDBHandler);
        fileToDBConsumer.initConsumer();
        String commitMode = "async";

        fileToDBConsumer.pollConsumes(1000, commitMode);

    }

}