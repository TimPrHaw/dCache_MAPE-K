import kafka.KafkaListener;
import kafka.KafkaProd;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

public class Main {
    public static void main(String[] args) {
        String topic = "test-topic";
        String bootstrapServer = "localhost:9092";

        KafkaProd producer = new KafkaProd(topic, bootstrapServer);
        /**KafkaListener consumer = new KafkaListener(topic, bootstrapServer);

        });
        
        new Thread( () ->
                consumer.run()).start();
        
        for (int i = 1 ; i <= 5 ; i++){
            producer.send("key-1", String.valueOf(i));
        }
         */
    }
}
