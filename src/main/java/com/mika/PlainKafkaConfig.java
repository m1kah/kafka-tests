package com.mika;

import com.mika.kafka.PlainConsumer;
import com.mika.processing.EventPrinter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class PlainKafkaConfig {
    @Bean
    PlainConsumer plainConsumer(
        @Value("${plain.bootstrap-servers}") String bootstrapServers,
        @Value("${plain.group-id:plain}") String groupId
    ) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServers);
        props.setProperty("group.id", groupId);
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new PlainConsumer(
                "ping",
                props,
                new EventPrinter()::print);
    }
}
