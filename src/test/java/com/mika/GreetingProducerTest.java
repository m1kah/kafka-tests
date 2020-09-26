package com.mika;

import io.restassured.RestAssured;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(SpringExtension.class)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@EmbeddedKafka(
        partitions = 1,
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        topics = {
                "hello-kafka",
                "greetings"
        }
)
@DirtiesContext
public class GreetingProducerTest {
    @LocalServerPort
    int port;
    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;
    TestConsumer<String, String> testConsumer;

    @BeforeEach
    public void setup() {
        RestAssured.port = port;
        testConsumer = new TestConsumer<>(embeddedKafkaBroker, "greetings");
    }

    @Test
    public void greet() throws Exception {
        RestAssured.when()
                .get("/greet/duke")
            .then()
                .log().ifValidationFails()
                .statusCode(200);

        ConsumerRecord<String, String> record = testConsumer.poll(Duration.ofSeconds(1));
        assertNotNull(record);
        assertEquals("Greetings, duke!", record.value());
    }
}
