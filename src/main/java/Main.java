import kafka.KafkaListener;
import kafka.KafkaProd;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import simulation.SimSensor;

import java.time.Duration;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        String topic = "Radiator-Temperature";
        String bootstrapServer = "localhost:9092";
        String key = "key_1";

        int simTicksInMilliSec = 5 * 1000;
        int simStartTemp = 20;



        new Thread(() -> {
            KafkaListener consumer = new KafkaListener(topic, bootstrapServer);
            consumer.run();
        }).start();


        new Thread(() -> {
            SimSensor simSensor = new SimSensor(simTicksInMilliSec, simStartTemp, topic, bootstrapServer, key);
            simSensor.run();
        }).start();
    }
}
