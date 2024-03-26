package kafka.consumer;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.producer.ProducerConfig;
import kafka.KafkaListener;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.ProducerRecord;


@Testcontainers
class KafkaListenerLiveTest {

    @Container
    private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));

    static {
        Awaitility.setDefaultTimeout(ofSeconds(5L));
        Awaitility.setDefaultPollInterval(ofMillis(50L));
    }

    // TODO: Den Listener richtig testen. ggf mit und ohne Kafka broker
    @Test
    void givenANewCustomKafkaListener_thenConsumesAllMessages() {
        // given
        String TEST_TOPIC = "test-topic-consumed-message";
        String bootstrapServers = KAFKA_CONTAINER.getBootstrapServers();
        List<String> consumedMessages = new ArrayList<>();

        // when
        KafkaListener listener = new KafkaListener(TEST_TOPIC, bootstrapServers);
        //KafkaListener listener = new KafkaListener(topic, bootstrapServers).onEach(consumedMessages::add);
        CompletableFuture.runAsync(listener);

        // and
        publishArticles(TEST_TOPIC,
        "1", "2", "3", "44", "55"
        );

        // then
        await().untilAsserted(() ->
        assertThat(consumedMessages).containsExactlyInAnyOrder(
                "1", "2", "3", "44", "55"
        ));


}

    private void publishArticles(String topic, String... articles) {
        try (KafkaProducer<String, String> producer = testKafkaProducer()) {
            Arrays.stream(articles)
                    .map(article -> new ProducerRecord<>(topic, "key-1", article))
                    .forEach(producer::send);
        }
    }

    private static KafkaProducer<String, String> testKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    /**
    @Test
    void mockTest(){
        String TEST_TOPIC = "test-topic-consumed-message";
        MockConsumer<String, String> mockConsumer =  new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        //mockConsumer.subscribe(Collections.singletonList(TEST_TOPIC));

        mockConsumer.assign(Collections.singleton(new TopicPartition(TEST_TOPIC, 0)));
        mockConsumer.seekToBeginning(Collections.singleton(new TopicPartition(TEST_TOPIC, 0)));

        // KafkaListener erstellen und mit dem MockConsumer initialisieren
        KafkaListener kafkaListener = new KafkaListener(TEST_TOPIC, mockConsumer);

        List<ConsumerRecord<String, String>> records = Arrays.asList(
                new ConsumerRecord<>(TEST_TOPIC, 0, 0, "key_1", "value1"),
                new ConsumerRecord<>(TEST_TOPIC, 0, 0, "key_1", "value1"),
                new ConsumerRecord<>(TEST_TOPIC, 0, 0, "key_1", "value1")
        );

        for (ConsumerRecord<String, String> record : records){
            mockConsumer.addRecord(record);
        }

        kafkaListener.run();

        assertEquals(3, mockConsumer.poll(Duration.ZERO).count());




    }
    */

    /**

    @Test
    void givenANewCustomKafkaListener_thenConsumesAllMessages() {
        // given
        String topic = "baeldung.articles.published";
        String bootstrapServers = KAFKA_CONTAINER.getBootstrapServers();
        List<String> consumedMessages = new ArrayList<>();

        // when
        KafkaListener listener = new KafkaListener(topic, bootstrapServers).onEach(consumedMessages::add);
        CompletableFuture.runAsync(listener);

        // and
        publishArticles(topic,
                "Introduction to Kafka",
                "Kotlin for Java Developers",
                "Reactive Spring Boot",
                "Deploying Spring Boot Applications",
                "Spring Security"
        );

        // then
        await().untilAsserted(() ->
                assertThat(consumedMessages).containsExactlyInAnyOrder(
                        "Introduction to Kafka",
                        "Kotlin for Java Developers",
                        "Reactive Spring Boot",
                        "Deploying Spring Boot Applications",
                        "Spring Security"
                ));
    }

    private void publishArticles(String topic, String... articles) {
        try (KafkaProducer<String, String> producer = testKafkaProducer()) {
            Arrays.stream(articles)
                    .map(article -> new ProducerRecord<>(topic, "key-1", article))
                    .forEach(producer::send);
        }
    }

    private static KafkaProducer<String, String> testKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
    }
         */
}