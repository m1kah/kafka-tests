package com.mika;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import mika.Note;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
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

import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

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
                "greetings",
                "notes"
        }
)
@DirtiesContext
public class NoteProducerTest {
    @LocalServerPort
    int port;
    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @BeforeEach
    public void setup() {
        RestAssured.port = port;
    }

    @Test
    public void sendNote() throws Exception {
        TestConsumer<String, Note> testConsumer = new TestConsumer<>(
                embeddedKafkaBroker,
                "notes",
                StringDeserializer.class,
                KafkaAvroDeserializer.class,
                Map.of("schema.registry.url", "mock://localhost"));

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body(
                    """
                    {
                        "from": "duke",
                        "to": "dude",
                        "message": "yo"
                    }
                    """)
            .when()
                .post("/note")
            .then()
                .log().ifValidationFails()
                .statusCode(200);

        ConsumerRecord<String, Note> record = testConsumer.poll(Duration.ofSeconds(1));
        assertNotNull(record);

        System.out.println("Record key  : " + record.key());
        System.out.println("Record value: " + record.value());
    }

    @Test
    public void sendNoteManualDeserialization() throws Exception {
        TestConsumer<String, byte[]> testConsumer = new TestConsumer<>(
                embeddedKafkaBroker,
                "notes",
                StringDeserializer.class,
                ByteArrayDeserializer.class,
                Map.of());

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body(
                        """
                        {
                            "from": "duke",
                            "to": "dude",
                            "message": "yo"
                        }
                        """)
                .when()
                .post("/note")
                .then()
                .log().ifValidationFails()
                .statusCode(200);

        ConsumerRecord<String, byte[]> record = testConsumer.poll(Duration.ofSeconds(1));
        assertNotNull(record);

        System.out.println("Record key  : " + record.key());

        var reader = new SpecificDatumReader<>(Note.getClassSchema());
        byte[] bytes = record.value();
        // https://docs.confluent.io/current/schema-registry/serdes-develop/index.html#wire-format
        // Byte 0 is magic byte
        // Bytes 1-4 are for schema ID
        // Bytes 5-n are for data
        var start = 5;
        var length = bytes.length - start;
        var decoder = DecoderFactory.get().binaryDecoder(bytes, start, length, null);
        var note = reader.read(null, decoder);
        System.out.println("Record value: " + note);
    }

    @Test
    public void avroSerializeDeserialize() throws Exception {
        // https://cwiki.apache.org/confluence/display/AVRO/FAQ#FAQ-Deserializingfromabytearray

        var note = new Note();
        note.setTitle("");
        note.setSent(Instant.now().toEpochMilli());
        note.setReceiver("me");
        note.setSender("me");
        note.setId("1");
        note.setNote("note");

        var out = new ByteArrayOutputStream();
        var encoder = EncoderFactory.get().binaryEncoder(out, null);
        var writer = new SpecificDatumWriter<>(Note.getClassSchema());
        writer.write(note, encoder);
        encoder.flush();
        out.close();
        var bytes = out.toByteArray();

        var reader = new SpecificDatumReader<>(Note.getClassSchema());
        var decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        var deserialized = reader.read(null, decoder);

        System.out.println(deserialized);
    }
}
