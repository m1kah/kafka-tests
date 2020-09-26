package com.mika.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class GreetingProducer {
    private static final Logger log = LoggerFactory.getLogger(GreetingProducer.class);
    final String topic;
    final KafkaProducer<String, String> kafkaProducer;

    public GreetingProducer(String topic, KafkaProducer<String, String> kafkaProducer) {
        this.topic = topic;
        this.kafkaProducer = kafkaProducer;
    }

    public void send(String name) {
        var future = kafkaProducer.send(new ProducerRecord<>(
                topic,
                "Greetings, %s!".formatted(name)));
        try {
            var recordMetadata = future.get(1, TimeUnit.SECONDS);
            log.info("Greeting sent name={} timestamp={} offset={} partition={}",
                    name,
                    recordMetadata.timestamp(),
                    recordMetadata.offset(),
                    recordMetadata.partition());

        } catch (Exception e) {
            try {
                future.cancel(true);
            } catch (RuntimeException e2) {
                log.warn("Failed to cancel greeting", e2);
            }
            throw new RuntimeException("Failed to send greeting", e);
        }
    }
}
