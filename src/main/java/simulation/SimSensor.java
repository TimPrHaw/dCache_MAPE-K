package simulation;

import kafka.KafkaProd;

import java.util.Random;
import java.util.logging.Logger;

public class SimSensor implements Runnable {
    private static final Logger log = Logger.getLogger(SimSensor.class.getName());
    private final KafkaProd PRODUCER;
    private final String KEY;
    private final int TICKS;
    private double temperature;
    private boolean heating = true;

    public SimSensor(int ticks, double temperature, String topic, String bootstrap, String kafkaKey) {
        TICKS = ticks;
        PRODUCER = new KafkaProd(topic, bootstrap);
        KEY = kafkaKey;
        this.temperature = temperature;
    }

    public void setTemperature(double temperature) {
        log.info("New temperatur: " + temperature);
        this.temperature = temperature;
    }

    public double getTemperature(){
        return temperature;
    }

    public void setHeating(boolean heating) {
        this.heating = heating;
    }

    @Override
    public void run() {
        log.info("Start Sensor Simulation");
        while (true){
            try {
                Thread.sleep(TICKS);
                double randomNummer = new Random().nextDouble() * 10;

                if (randomNummer > 9){
                    setTemperature(100);
                } else if (randomNummer < 1) {
                    setTemperature(-1);
                } else {
                    setTemperature(heating ? getTemperature() + new Random().nextDouble() : getTemperature() - new Random().nextDouble());
                }
                PRODUCER.send(KEY, Double.toString(getTemperature()));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }
    }
}
