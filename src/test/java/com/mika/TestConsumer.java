package com.mika;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class TestConsumer<K, V> {
    private LinkedBlockingDeque<ConsumerRecord<K, V>> records = new LinkedBlockingDeque<>();

    TestConsumer(
            EmbeddedKafkaBroker embeddedKafkaBroker,
            String topic) {
        this(embeddedKafkaBroker, topic, IntegerDeserializer.class, StringDeserializer.class, Map.of());
    }

    TestConsumer(
            EmbeddedKafkaBroker embeddedKafkaBroker,
            String topic,
            Class<? extends Deserializer<?>> keyDeserializer,
            Class<? extends Deserializer<?>> valueDeserializer,
            Map<String, Object> additionalProps) {
        var consumerProps = KafkaTestUtils.consumerProps("test", "false", embeddedKafkaBroker);
        consumerProps.put("key.deserializer", keyDeserializer);
        consumerProps.put("value.deserializer", valueDeserializer);
        consumerProps.putAll(additionalProps);
        var consumerFactory = new DefaultKafkaConsumerFactory<K, V>(consumerProps);
        var containerProps = new ContainerProperties(topic);
        var container = new KafkaMessageListenerContainer<>(consumerFactory, containerProps);
        container.setupMessageListener((MessageListener<K, V>) records::add);
        container.start();
        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
    }

    public ConsumerRecord<K, V> poll(Duration timeout) throws InterruptedException {
        return records.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

}
