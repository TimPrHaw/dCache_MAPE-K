package phases.plan;

import JMS.Producer;
import JMS.Consumer;
import phases.analyze.Analyze;

import javax.jms.JMSException;
import javax.jms.*;
import java.util.logging.Logger;


public class Plan {
    private static final Logger log = Logger.getLogger(Analyze.class.getName());
    private Consumer consumer = null;
    private Producer producer = null;
    private int upperThreshold = 25;
    private int lowerThreshold = 18;

    public Plan(boolean queueBool, String inputQueue, String outputQueue) throws JMSException {
        this.consumer = new Consumer();
        this.producer = new Producer();
        consumer.setup(queueBool, inputQueue);
        producer.setup(queueBool, outputQueue);
    }

    public void run() throws JMSException {
        log.info(this.getClass().getName() + " is running...");
        while (true) {
            var message = consumer.receive();
            double tmp = (double)((ObjectMessage)message).getObject();
            checkMessageThenSend(planing(tmp));
        }
    }

    public void setUpperThreshold(int upperThreshold) {
        log.info(this.getClass().getName() + " is setting upper threshold to " + upperThreshold);
        this.upperThreshold = upperThreshold;
    }

    public void setLowerThreshold(int lowerThreshold) {
        log.info(this.getClass().getName() + " is setting lower threshold to " + lowerThreshold);
        this.lowerThreshold = lowerThreshold;
    }

    private String planing(double input){
        String res = "okay";
        if (input > upperThreshold + 10 || input < lowerThreshold - 10) {
            res = "reset";
        } else if (input > upperThreshold){
            res = "toHigh";
        } else if (input < lowerThreshold){
            res = "toLow";
        }
        return res;
    }

    private void checkMessageThenSend(String message){
        if (!message.equals("okay")){
            try {
                log.info(this.getClass().getName() + " sending message " + message);
                producer.sendMessage(message);
            } catch (JMSException e) {}
        }
    }
}
