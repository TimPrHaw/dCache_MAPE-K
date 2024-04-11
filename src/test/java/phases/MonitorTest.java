package phases;

import org.apache.activemq.*;

import javax.jms.*;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import phases.monitor.Monitor;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.Arrays;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Testcontainers
public class MonitorTest {


    @Container
    private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));

    static {
        Awaitility.setDefaultTimeout(ofSeconds(5L));
        Awaitility.setDefaultPollInterval(ofMillis(50L));
    }

    //TODO: Add json test

    @Test
    public void testMonitor() throws JMSException {
        String topicName = "test-topic-kafka-monitor";
        String queueName = "analyze-queue";
        String bootstrapServers = KAFKA_CONTAINER.getBootstrapServers();

        Monitor monitor = new Monitor("kafka", topicName, bootstrapServers);
        consumerClass consumer = new consumerClass(queueName);

        publishStrings(topicName, "01", "02", "03", "04", "05", "06", "07", "08", "09", "10");

        CompletableFuture.runAsync(monitor::run);
        await().untilAsserted(() ->
                        consumer.receiveMsg());

        String testMessage = consumer.getMsg();
        Assert.assertEquals(testMessage, "[7.0, 8.0, 9.0, 10.0]");
    }

    private void publishStrings(String topic, String... articles) {
        try (KafkaProducer<String, String> producer = testKafkaProducer()) {
            Arrays.stream(articles)
                    .map(article -> new ProducerRecord<>(topic, "key-1", article))
                    .forEach(producer::send);
        }
    }

    private static KafkaProducer<String, String> testKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        //props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private class consumerClass{
        public String msg;
        MessageConsumer consumer;

        consumerClass(String queueName) throws JMSException {
            Connection testConsumerConnection = new ActiveMQConnectionFactory().createConnection();
            testConsumerConnection.start();
            Session testConsumerSession = testConsumerConnection.createSession();
            Destination testConsumerDestination = testConsumerSession.createQueue(queueName);
            this.consumer = testConsumerSession.createConsumer(testConsumerDestination);
        }

        public void receiveMsg() throws JMSException {
            this.msg = ((TextMessage)consumer.receive()).getText();
        }

        public String getMsg(){
            return this.msg;
        }
    }
}

