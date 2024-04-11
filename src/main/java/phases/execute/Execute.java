package phases.execute;

import JMS.Producer;
import JMS.Consumer;
import phases.analyze.Analyze;

import javax.jms.JMSException;
import java.util.logging.Logger;

public class Execute {
    private static final Logger log = Logger.getLogger(Analyze.class.getName());
    private Consumer consumer = null;
    private Producer producer = null;
    private double value;

    public Execute(boolean queueBool, String inputQueue, String outputQueue) throws JMSException {
        this.consumer = new Consumer();
        this.producer = new Producer();
        consumer.setup(queueBool, inputQueue);
        producer.setup(queueBool, outputQueue);
    }

    public void run() throws JMSException {
        while (true) {
            var msg = consumer.run();
            exec((String) msg);
            producer.sendMessage(getValue());
        }
    }

    public double getValue() {
        return value;
    }

    private void exec(String input){
        switch (input) {
            case "reset":
                this.value = -1;
                break;
            case "toHigh":
                this.value = 3;
                break;
            case "toLow":
                this.value = 2;
                break;
            case "okay":
                this.value = 1;
                break;
            default:
                this.value = 0;
        }
        log.info(this.getClass().getName() + " executed: " + input);
    }
}
