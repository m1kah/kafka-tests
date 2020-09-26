package com.mika;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class TestConsumer<K, V> {
    private LinkedBlockingDeque<ConsumerRecord<K, V>> records = new LinkedBlockingDeque<>();

    TestConsumer(
            EmbeddedKafkaBroker embeddedKafkaBroker,
            String topic) {
        var consumerProps = KafkaTestUtils.consumerProps("test", "false", embeddedKafkaBroker);
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
