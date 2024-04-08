package simulation;

import JMS.Producer;
import JMS.SynchConsumer;
import phases.analyze.Analyze;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import java.util.logging.Logger;

public class SimActor{
    private static final Logger log = Logger.getLogger(Analyze.class.getName());
    private SynchConsumer consumer = null;
    private Producer producer = null;
    private int resetvalue;

    public SimActor(boolean queueBool, String inputQueue, String outputQueue, int resetvalue) throws JMSException {
        this.consumer = new SynchConsumer();
        this.producer = new Producer();
        this.resetvalue = resetvalue;
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
        producer.sendMessage(resetvalue);
    }

    private void setHeating(int value) throws JMSException {
        if (value == 2) {
            producer.sendMessage(true);
        } else if (value == 3) {
            producer.sendMessage(false);
        }
    }
}
