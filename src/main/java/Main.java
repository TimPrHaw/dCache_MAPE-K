import kafka.KafkaCons;
import phases.analyze.Analyze;
import phases.execute.Execute;
import phases.monitor.Monitor;
import phases.plan.Plan;
import simulation.SimActor;
import simulation.SimSensor;

import javax.jms.JMSException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        String topic = "Radiator-Temperature";
        String bootstrapServer = "localhost:9092";
        String key = "key_1";
        boolean queueBool = true;

        // Start Monitor
        new Thread(() -> {
            Monitor monitor;
            try {
                monitor = new Monitor("billingrecords");
                monitor.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }).start();
        // Start Analyze

        new Thread(() -> {
            Analyze analyze;
            try {
                analyze = new Analyze();
                analyze.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }).start();
        // Start Plan
        new Thread(() -> {
            Plan plan;
            try {
                plan = new Plan();
                plan.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }).start();
        // Start Execute
        new Thread(() -> {
            Execute execute = null;
            try {
                execute = new Execute();
                execute.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }).start();
    }
}
