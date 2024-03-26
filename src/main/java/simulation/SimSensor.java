package simulation;

import kafka.KafkaProd;

import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SimSensor {
    private static final Logger log = Logger.getLogger(SimSensor.class.getName());
    private final KafkaProd producer;
    private final String KEY = "temperature";
    private final int TICKS;
    private double temp;
    private boolean heating = true;

    public SimSensor(int ticks, double temp, String topic, String bootstrap) {
        TICKS = ticks;
        producer = new KafkaProd(topic, bootstrap);
        this.temp = temp;
    }

    public void setTemp(double temp) {
        log.info("New temperatur: " + temp);
        this.temp = temp;
    }

    public double getTemp(){
        return temp;
    }

    public void startSensorSimulation() throws InterruptedException{
        log.info("Start Sensor Simulation");

        while (true){
            Thread.sleep(TICKS);
            double randomNummer = new Random().nextDouble() * 10;

            if (randomNummer > 9){
                setTemp(100);
            } else if (randomNummer < 1) {
                setTemp(-1);
            } else {
                setTemp(heating ? getTemp() + new Random().nextDouble() : getTemp() - new Random().nextDouble());
            }
            // TODO: send temperature to KAFKA


        }
    }
}
