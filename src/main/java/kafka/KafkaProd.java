package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProd {
    private final String topic;
    //private KafkaProducer<String, String> producer;
    private Producer<String, String> producer;

    public KafkaProd(String topic, Producer<String, String> producer){
        this.topic = topic;
        this.producer = producer;
    }

    public KafkaProd(String topic, String bootstrapServers) {
        this(topic, defaultKafkaProducer(bootstrapServers));
    }

    private static Producer<String, String> defaultKafkaProducer(String bootstrapServers){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public void send(String key, String value) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
        producer.send(producerRecord);
    }
}
