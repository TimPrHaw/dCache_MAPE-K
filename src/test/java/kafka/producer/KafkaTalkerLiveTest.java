package kafka.producer;

import kafka.KafkaProd;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;
// import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.*;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

@Testcontainers
class KafkaTalkerLiveTest {

    @Container
    private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));
    static {
        Awaitility.setDefaultTimeout(ofSeconds(5L));
        Awaitility.setDefaultPollInterval(ofMillis(50L));
    }

    /**
    @Test
    void givenANewKafkaProducer_thenSendsAMessage(){

        KafkaProd producer;
        MockConsumer<String, String> mockConsumer;


        String topic = "test-topic-produced-message";
        String bootstrapServers = KAFKA_CONTAINER.getBootstrapServers();

        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        mockConsumer.subscribe(Collections.singleton(topic));

        producer = new KafkaProd(topic, bootstrapServers);

        producer.send("key1", "value1");

        ConsumerRecords<String, String> records = mockConsumer.poll(Duration.ofMillis(100));
        assertEquals(1, records.count());

        ConsumerRecord<String, String> receivedRecord = records.iterator().next();
        assertEquals("test-topic-produced-message", receivedRecord.topic());
        assertEquals("key1", receivedRecord.key());
        assertEquals("value1", receivedRecord.value());

    }

    private static KafkaConsumer<String, String> testKafkaConsumer(){
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test_group_id");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    private ArrayList<String> getTestMessages(String topic) {
        ArrayList<String> list = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = testKafkaConsumer()) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(ofMillis(100)).forEach(record -> list.add(String.valueOf(record)));
        }
        return list;
    }
     */
}
