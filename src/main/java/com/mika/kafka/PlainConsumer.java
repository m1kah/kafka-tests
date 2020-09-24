package com.mika.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class PlainConsumer implements AutoCloseable {
    private static Logger log = LoggerFactory.getLogger(PlainConsumer.class);
    private String topic;
    private Consumer<String> eventProcessor;
    private KafkaConsumer<String, String> consumer;
    private ExecutorService executorService;
    private AtomicBoolean closed = new AtomicBoolean(false);

    public PlainConsumer(String topic, Properties props, Consumer<String> eventProcessor) {
        this.topic = topic;
        this.eventProcessor = eventProcessor;
        consumer = new KafkaConsumer<>(props);
        executorService = Executors.newSingleThreadExecutor(new NamingThreadFactory("kc-plain"));
        executorService.submit(this::pollKafka);
    }

    private void pollKafka() {
        consumer.subscribe(List.of(topic));
        log.info("Plain consumer started topic={}", topic);
        try {
            while (!closed.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) {
                    log.debug("No records, polling again");
                    continue;
                }
                Instant start = Instant.now();
                records.forEach(record -> eventProcessor.accept(record.value()));
                consumer.commitSync();
                Duration duration = Duration.between(start, Instant.now());
                log.info("Committed after processing {} records in {} ms",
                        records.count(),
                        duration.toMillis());
            }
        } catch (WakeupException e) {
            if (!closed.get()) throw e;
        } finally {
            consumer.close();
        }
    }

    @Override
    public void close() throws Exception {
        closed.set(true);
        consumer.wakeup();
        log.info("Stopping kafka consumer");
    }
}
