package simulation;

import JMS.Producer;
import JMS.Consumer;
import phases.analyze.Analyze;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import java.util.logging.Logger;

public class SimActor{
    private static final Logger log = Logger.getLogger(Analyze.class.getName());
    private Consumer consumer = null;
    private Producer producer = null;
    private final int resetValue;

    public SimActor(boolean queueBool, String inputQueue, String outputQueue, int resetValue) throws JMSException {
        this.consumer = new Consumer();
        this.producer = new Producer();
        this.resetValue = resetValue;
        consumer.setup(queueBool, inputQueue);
        producer.setup(queueBool, outputQueue);
    }

    public void run() throws JMSException {
        log.info(this.getClass().getName()+" is running...");

        while(true){
            var value = consumer.run();
            if (value instanceof ObjectMessage){
                double msg = Double.parseDouble(((ObjectMessage)value).getObject().toString());
                if (msg > 0){
                    setHeating((int)msg);
                } else {
                    resetTemperature();
                }
            }
        }
    }

    private void resetTemperature() throws JMSException {
        log.info(this.getClass().getName()+" is resetting the temperature: " + resetValue);
        producer.sendMessage(resetValue);
    }

    private void setHeating(int value) throws JMSException {
        if (value == 2) {
            log.info(this.getClass().getName()+ " is now heating...");
            producer.sendMessage(true);
        } else if (value == 3) {
            log.info(this.getClass().getName()+ " is now cooling...");
            producer.sendMessage(false);
        }
    }

}
