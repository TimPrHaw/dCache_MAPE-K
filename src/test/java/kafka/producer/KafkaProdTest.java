package kafka.producer;

import static org.assertj.core.api.Assertions.assertThat;

import kafka.KafkaProd;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class KafkaProdTest {
    private static final String TEST_TOPIC = "test-topic-produced-message";
    MockProducer<String, String> mockProducer =
            new MockProducer<>(true, new StringSerializer(), new StringSerializer());
    KafkaProd processor = new KafkaProd(TEST_TOPIC, mockProducer);
    @Test
    void sendingTwoValues_checkSendOrder(){
        String value1 = "1";
        String value2 = "2";
        String key1 = "key_1";

        processor.send(key1, value1);
        processor.send(key1, value2);

        assertThat(mockProducer.history()).hasSize(2);

        ProducerRecord<String, String> firstValueRecord = mockProducer.history().get(0);
        assertThat(firstValueRecord.value().contains(value1));
        assertThat(firstValueRecord.topic()).isEqualTo(TEST_TOPIC);

        ProducerRecord<String, String> secondValueRecord = mockProducer.history().get(1);
        assertThat(secondValueRecord.value().contains(value1));
        assertThat(secondValueRecord.topic()).isEqualTo(TEST_TOPIC);
    }
}
