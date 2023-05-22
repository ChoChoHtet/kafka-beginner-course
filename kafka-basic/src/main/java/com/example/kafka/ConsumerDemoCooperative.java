package com.example.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoCooperative {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka Consumer Demo with Cooperative");
        String groupId = "my-java-application";
        String topic = "demo_java";

        //create Producer properties
        Properties properties = new Properties();

        //connect to Localhost
        //properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        //connect to Conduktor Playground
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"1oaJvXlKltR7n2Uybt4rPl\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIxb2FKdlhsS2x0UjduMlV5YnQ0clBsIiwib3JnYW5pemF0aW9uSWQiOjczMjk1LCJ1c2VySWQiOjg1MjE0LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiI1NTQ2MDBmYy1kNDNlLTRkYTMtOTBmYi01NWJiMjVlMTY5NGEifX0.scwLiPIn-dTDohJykg802c5UvMK8aUKeWLBvsalwzpY\";");
        properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");

        //create consumer property config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);

        /**
         * none - if we don't have any existing consumer group , then it wil fail.
         * we must set consumer groups before we start application
         * earliest - read from the beginning of topic
         * latest - only read from just now and only from new messages sent from now
         */
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
        //properties.setProperty("group.instance.id", "...."); // strategy for static assignments

        //create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //get a reference to the main thread
        Thread mainThread = Thread.currentThread();

        //adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown , let's exit by calling consumer wakeup()");
            consumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            //subscribe to topic
            consumer.subscribe(List.of(topic));
            //poll for data in infinite loop
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("key: " + record.key() + " ,value: " + record.value());
                    log.info("partition: " + record.partition() + " ,offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shutdown");
        } finally {
            consumer.close(); // close consumer , this will also commit offsets
            log.info("Consumer is now gracefully shut down");
        }


    }
}
