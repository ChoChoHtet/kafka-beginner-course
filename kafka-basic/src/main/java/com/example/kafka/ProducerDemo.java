package com.example.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Hello World ");

        //create Producer properties
        Properties properties = new Properties();

        //connect to Localhost
        //properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        //connect to Conduktor Playground
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"1oaJvXlKltR7n2Uybt4rPl\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIxb2FKdlhsS2x0UjduMlV5YnQ0clBsIiwib3JnYW5pemF0aW9uSWQiOjczMjk1LCJ1c2VySWQiOjg1MjE0LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiI1NTQ2MDBmYy1kNDNlLTRkYTMtOTBmYi01NWJiMjVlMTY5NGEifX0.scwLiPIn-dTDohJykg802c5UvMK8aUKeWLBvsalwzpY\";");
        properties.setProperty("sasl.mechanism","PLAIN");
        properties.setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");

        //set Producer properties serializer
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        //create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create a Producer record
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("demo_java","Hello world");

        //send data to kafka
        producer.send(producerRecord);

        //tell the producer to send all data and block until it's done - synchronous
        producer.flush();

        //flush and close the producer
        producer.close();
    }
}
