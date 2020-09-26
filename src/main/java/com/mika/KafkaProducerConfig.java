package com.mika;

import com.mika.kafka.GreetingProducer;
import com.mika.kafka.NoteProducer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import mika.Note;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;
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

    @Bean
    NoteProducer noteProducer(
            @Value("${spring.kafka.bootstrap-servers:notes}") String bootstrapServers,
            @Value("${note.topic:notes}") String topic,
            @Value("${avro.schema-registry-url}") String schemaRegistryUrl
    ) {
        var props = Map.<String, Object>of(
                "bootstrap.servers", bootstrapServers,
                "key.serializer", StringSerializer.class,
                "value.serializer", KafkaAvroSerializer.class,
                "schema.registry.url", schemaRegistryUrl);
        var factory = new DefaultKafkaProducerFactory<String, Note>(props);
        var kafkaTemplate = new KafkaTemplate<>(factory);
        return new NoteProducer(topic, kafkaTemplate);
    }
}
