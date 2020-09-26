package com.mika.kafka;

import mika.Note;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.TimeUnit;

public class NoteProducer {
    private static final Logger log = LoggerFactory.getLogger(NoteProducer.class);
    final String topic;
    final KafkaTemplate<String, Note> noteKafkaTemplate;

    public NoteProducer(
            String topic,
            KafkaTemplate<String, Note> noteKafkaTemplate
    ) {
        this.topic = topic;
        this.noteKafkaTemplate = noteKafkaTemplate;
    }

    public void send(Note note) {
        var future = noteKafkaTemplate.send(topic, note.getId().toString(), note);
        try {
            var res = future.get(1, TimeUnit.SECONDS);
            var recordMetadata = res.getRecordMetadata();
            log.info("Note sent id={} timestamp={} offset={} partition={}",
                    res.getProducerRecord().value().getId(),
                    recordMetadata.timestamp(),
                    recordMetadata.offset(),
                    recordMetadata.partition());
        } catch (Exception e) {
            try {
                future.cancel(true);
            } catch (Exception e2) {
                log.warn("Failed to cancel kafka producer", e2);
            }
            throw new RuntimeException("Failed to send note", e);
        }
    }
}
