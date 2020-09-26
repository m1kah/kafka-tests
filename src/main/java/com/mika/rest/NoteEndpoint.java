package com.mika.rest;

import com.mika.kafka.NoteProducer;
import mika.Note;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.UUID;

@RestController
public class NoteEndpoint {
    final NoteProducer noteProducer;

    public NoteEndpoint(NoteProducer noteProducer) {
        this.noteProducer = noteProducer;
    }

    @PostMapping("note")
    public ResponseEntity<?> postNote(@RequestBody JsonNote note) {
        noteProducer.send(map(note));
        return ResponseEntity.ok().build();
    }

    private Note map(JsonNote source) {
        var dest = new Note();
        dest.setId(UUID.randomUUID().toString());
        dest.setSender(source.from);
        dest.setReceiver(source.to);
        dest.setSent(Instant.now().toEpochMilli());
        dest.setNote(source.message);
        dest.setTitle("");
        return dest;
    }

    static class JsonNote {
        public String from;
        public String to;
        public String message;
    }
}
