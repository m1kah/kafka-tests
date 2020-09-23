package com.mika;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(SpringExtension.class)
@SpringBootTest(
        properties = {
                "logging.level.kafka=WARN",
                "logging.level.org.apache.kafka=WARN",
                "logging.level.org.apache.zookeper=WARN"
        }
)
@EmbeddedKafka(
        partitions = 1,
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        topics = {
                "hello-kafka"
        }
)
@DirtiesContext
public class HelloKafkaTest {
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    public void produceAndConsume() throws Exception {
        var consumerProps = KafkaTestUtils.consumerProps("test", "false", embeddedKafkaBroker);
        var consumerFactory = new DefaultKafkaConsumerFactory<String, String>(consumerProps);
        var containerProps = new ContainerProperties("hello-kafka");
        var container = new KafkaMessageListenerContainer<String, String>(consumerFactory, containerProps);
        var records = new LinkedBlockingDeque<ConsumerRecord<String, String>>();
        container.setupMessageListener((MessageListener<String, String>) records::add);
        container.start();
        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());

        var producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        var producerFactory = new DefaultKafkaProducerFactory<String, String>(producerProps);
        var template = new KafkaTemplate<String, String>(producerFactory);

        template.send("hello-kafka", "HELLO!");
        var record = records.poll(1, TimeUnit.SECONDS);
        assertNotNull(record);
        System.out.println(record.value());
    }
}
