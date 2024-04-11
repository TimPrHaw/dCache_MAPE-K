import kafka.KafkaCons;
import phases.analyze.Analyze;
import phases.execute.Execute;
import phases.monitor.Monitor;
import phases.plan.Plan;
import simulation.SimActor;
import simulation.SimSensor;

import javax.jms.JMSException;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        String topic = "Radiator-Temperature";
        String bootstrapServer = "localhost:9092";
        String key = "key_1";
        boolean queueBool = true;

        int simTicksInMilliSec = 3 * 1000;
        int simStartTemp = 20;

        // Start Monitor
        new Thread(() -> {
            Monitor monitor;
            try {
                monitor = new Monitor("kafka", topic, bootstrapServer);
                monitor.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }).start();
        // Start Analyze
        new Thread(() -> {
            Analyze analyze;
            try {
                analyze = new Analyze(queueBool, "analyze-queue", "plan-queue");
                analyze.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }).start();
        // Start Plan
        new Thread(() -> {
            Plan plan;
            try {
                plan = new Plan(queueBool, "plan-queue", "execute-queue");
                plan.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }).start();
        // Start Execute
        new Thread(() -> {
            Execute execute = null;
            try {
                execute = new Execute(queueBool, "execute-queue", "actor-queue");
                execute.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }).start();
        // Start actor simulation
        new Thread(() -> {
            SimActor simActor;
            try {
                simActor = new SimActor(queueBool, "actor-queue", "sim-queue", simStartTemp);
                simActor.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }).start();
        // Start sensor simulation
        new Thread(() -> {
            SimSensor simSensor;
            try {
                simSensor = new SimSensor(simTicksInMilliSec, simStartTemp, topic, bootstrapServer, key, queueBool, "sim-queue");
                simSensor.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }).start();
    }
}
