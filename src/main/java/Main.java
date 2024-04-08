import kafka.KafkaListener;
import kafka.KafkaProd;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import phases.analyze.Analyze;
import phases.execute.Execute;
import phases.plan.Plan;
import simulation.SimActor;
import simulation.SimSensor;

import javax.jms.JMSException;
import java.time.Duration;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        String topic = "Radiator-Temperature";
        String bootstrapServer = "localhost:9092";
        String key = "key_1";
        boolean queueBool = true;

        int simTicksInMilliSec = 3 * 1000;
        int simStartTemp = 20;



        new Thread(() -> {
            KafkaListener consumer = new KafkaListener(topic, bootstrapServer);
            consumer.run();
        }).start();

        new Thread(() -> {
            Analyze analyze = null;
            try {
                analyze = new Analyze(queueBool, "analyze-queue", "plan-queue");
                analyze.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }).start();

        new Thread(() -> {
            Plan plan = null;
            try {
                plan = new Plan(queueBool, "plan-queue", "execute-queue");
                plan.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }).start();

        new Thread(() -> {
            Execute execute = null;
            try {
                execute = new Execute(queueBool, "execute-queue", "actor-queue");
                execute.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }).start();

        new Thread(() -> {
            SimActor simActor = null;
            try {
                simActor = new SimActor(queueBool, "actor-queue", "sim-queue", simStartTemp);
                simActor.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }).start();

        new Thread(() -> {
            SimSensor simSensor = null;
            try {
                simSensor = new SimSensor(simTicksInMilliSec, simStartTemp, topic, bootstrapServer, key, queueBool, "sim-queue");
                simSensor.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }).start();
    }
}
