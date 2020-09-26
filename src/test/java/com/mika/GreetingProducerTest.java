package com.mika;

import com.mika.kafka.GreetingProducer;
import io.restassured.RestAssured;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;

@ExtendWith(SpringExtension.class)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@EmbeddedKafka(
        partitions = 1,
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        topics = {
                "hello-kafka",
                "greetings",
                "notes"
        }
)
@DirtiesContext
public class GreetingProducerTest {
    @LocalServerPort
    int port;
    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;
    @SpyBean
    GreetingProducer greetingProducer;

    @BeforeEach
    public void setup() {
        RestAssured.port = port;
    }

    @Test
    public void greet() throws Exception {
        Mockito.doCallRealMethod().when(greetingProducer).send(any());
        var testConsumer = new TestConsumer<String, String>(embeddedKafkaBroker, "greetings");

        RestAssured.when()
                .get("/greet/duke")
            .then()
                .log().ifValidationFails()
                .statusCode(200);

        ConsumerRecord<String, String> record = testConsumer.poll(Duration.ofSeconds(1));
        assertNotNull(record);
        assertEquals("Greetings, duke!", record.value());
    }

    @Test
    public void greetFails() throws Exception {
        Mockito.doThrow(new RuntimeException("kafka error")).when(greetingProducer).send(any());

        RestAssured.when()
                .get("/greet/duke")
            .then()
                .log().ifValidationFails()
                .statusCode(500);
    }

}
