package com.mika;

import com.mika.kafka.GreetingProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class KafkaProducerConfig {
    @Bean
    GreetingProducer greetingProducer(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${greeting.topic:greetings}") String topic
    ) {
        var props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServers);
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        var kafkaProducer = new KafkaProducer<String, String>(props);
        return new GreetingProducer(topic, kafkaProducer);
    }
}
