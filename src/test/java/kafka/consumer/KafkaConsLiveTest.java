package kafka.consumer;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.producer.ProducerConfig;
import kafka.KafkaCons;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import java.util.Properties;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.ProducerRecord;


@Testcontainers
class KafkaConsLiveTest {

    @Container
    private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));

    static {
        Awaitility.setDefaultTimeout(ofSeconds(5L));
        Awaitility.setDefaultPollInterval(ofMillis(50L));
    }

    @Test
    void givenANewCustomKafkaListener_thenConsumesAllMessages() {
        String testTopic = "test-topic-consumed-message";
        String bootstrapServers = KAFKA_CONTAINER.getBootstrapServers();
        List<String> consumedMessages = new ArrayList<>();

        KafkaCons cons = new KafkaCons(testTopic, bootstrapServers);

        publishMessage(testTopic,"1", "2", "3", "44", "55");
        consumedMessages = cons.messageReceive();

        Assert.assertEquals("[1, 2, 3, 44, 55]", consumedMessages.toString());
}

    private void publishMessage(String topic, String... articles) {
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