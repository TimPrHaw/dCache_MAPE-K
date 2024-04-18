package simulation;

import JMS.Consumer;
import kafka.KafkaProd;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import java.util.Random;
import java.util.logging.Logger;

public class SimSensor implements Runnable{
    private static final Logger log = Logger.getLogger(SimSensor.class.getName());
    private final KafkaProd PRODUCER;
    private final String KEY;
    private final int TICKS;
    private double temperature;
    private boolean heating = true;
    private Consumer consumer;
    private boolean lastMsgReset;

    public SimSensor(int ticks, double temperature, String topic, String bootstrap, String kafkaKey ,boolean queueBool, String inputQueue) throws JMSException {
        TICKS = ticks;
        PRODUCER = new KafkaProd(topic, bootstrap);
        this.consumer = new Consumer();
        KEY = kafkaKey;
        this.temperature = temperature;
        this.consumer.setup(queueBool, inputQueue);
    }

    public void setTemperature(double temperature) {
        log.info("New temperatur: " + temperature);
        this.temperature = temperature;

    }

    public double getTemperature(){
        return temperature;
    }

    public void setHeating(boolean heating) {
        if (this.heating != heating){
            log.info(this.getClass().getName() + " mode has change to: " + (heating? "heating" : "cooling"));
            this.heating = heating;
        }
    }

    @Override
    public void run() {
        log.info("Start Sensor Simulation");
        consumer.setTimeout(10);
        while (true){
            try {
                Thread.sleep(TICKS);
                consumeMsg();
                double randomNummer = new Random().nextDouble() * 100;

                if (randomNummer > 99){
                    setTemperature(100);
                } else if (randomNummer < 1) {
                    setTemperature(-1);
                } else {
                    setTemperature(heating ? getTemperature() + new Random().nextDouble() : getTemperature() - new Random().nextDouble());
                }
                PRODUCER.send(KEY, Double.toString(getTemperature()));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }
    }
    private void consumeMsg() throws JMSException {
        var msg = consumer.receive();
        if (msg != null) {
            var val = ((ObjectMessage) msg).getObject();
            if (val instanceof Boolean) {
                this.lastMsgReset = false;
                setHeating((boolean) val);
            } else if (val instanceof Integer) {
                if (!lastMsgReset) {
                    log.info(this.getClass().getName() + " reset Sensor to: " + (int) val);
                    this.lastMsgReset = true;
                    setTemperature((int) val);
                }
            }
        }
    }
}
