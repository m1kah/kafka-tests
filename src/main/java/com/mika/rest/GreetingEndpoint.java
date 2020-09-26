package com.mika.rest;

import com.mika.kafka.GreetingProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class GreetingEndpoint {
    final GreetingProducer greetingProducer;

    public GreetingEndpoint(GreetingProducer greetingProducer) {
        this.greetingProducer = greetingProducer;
    }

    @GetMapping("greet/{name}")
    public ResponseEntity<?> greet(@PathVariable String name) {
        greetingProducer.send(name);
        return ResponseEntity.ok().build();
    }
}
