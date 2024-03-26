package kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaListener implements Runnable {
    private static final Logger log = Logger.getLogger(KafkaListener.class.getName());
    private final String topic;
    private final KafkaConsumer<String, String> consumer;
    private Consumer<String> recordConsumer;


    public KafkaListener(String topic, KafkaConsumer<String, String> consumer) {
        this.topic = topic;
        this.consumer = consumer;
        //this.recordConsumer = record -> log.info("received: " + record);
    }

    public KafkaListener(String topic, String bootstrapServers) {
        this(topic, defaultKafkaConsumer(bootstrapServers));
    }

    private static KafkaConsumer<String, String> defaultKafkaConsumer(String boostrapServers) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-id-group-1");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    @Override
    public void run() {
        consumer.subscribe(Collections.singletonList(topic));
        log.info(KafkaListener.class.getName() + " subscribed to: " + topic);
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                log.info(record.value());
            }
            consumer.commitSync(); // commitAsync(): falls Latenz reduziert oder der Durchsatz erhoeht werden soll

        }
    }


    /**
    public KafkaListener onEach(Consumer<String> newConsumer) {
        recordConsumer = recordConsumer.andThen(newConsumer);
        return this;
    }

    @Override
    public void run() {
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            consumer.poll(Duration.ofMillis(100))
                    .forEach(record -> recordConsumer.accept(record.value()));
        }
    }
    */

}