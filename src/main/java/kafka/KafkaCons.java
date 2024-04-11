package kafka;

import java.time.Duration;
import java.util.*;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import phases.monitor.MessageReceiver;


public class KafkaCons implements MessageReceiver {
    private static final Logger log = Logger.getLogger(KafkaCons.class.getName());
    private final String topic;
    private final Consumer<String, String> consumer;


    public KafkaCons(String topic, Consumer<String, String> consumer) {
        this.topic = topic;
        this.consumer = consumer;
    }

    public KafkaCons(String topic, String bootstrapServers) {
        this(topic, defaultKafkaConsumer(bootstrapServers));
    }

    public KafkaCons(String topic) {
        this(topic, defaultKafkaConsumer("localhost:9092"));
    }

    private static Consumer<String, String> defaultKafkaConsumer(String boostrapServers) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-id-group-1");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    private void subscribe() {
        consumer.subscribe(Collections.singletonList(topic));
        log.info(this.getClass().getName() + " subscribed to: " + topic);
    }

    @Override
    public List<String> messageReceive() {
        if (consumer.subscription().isEmpty()) {
            subscribe();
        }
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            if (!records.isEmpty()) {
                List<String> recordList = new ArrayList<>();
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Get Value: " + record.value());
                    recordList.add(record.value());
                }
                consumer.commitSync(); // commitAsync(): falls Latenz reduziert oder der Durchsatz erhoeht werden soll
                return recordList;
            }
        }
    }

    @Override
    public String toString() {
        return "KafkaCons{" +
                "topic='" + topic + '\'' +
                ", consumer=" + consumer.toString() +
                '}';
    }
}