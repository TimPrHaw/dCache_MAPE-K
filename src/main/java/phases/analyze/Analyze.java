package phases.analyze;

import JMS.Producer;
import JMS.Consumer;
import org.json.JSONObject;

import javax.jms.JMSException;
import java.util.logging.Logger;

public class Analyze implements Runnable{
    private static final Logger log = Logger.getLogger(Analyze.class.getName());
    private Consumer consumer = null;
    private Producer producer = null;

    public Analyze(boolean isDestinationQueue, String subscribedChannel, String publishedChannel) throws JMSException {
        this.consumer = new Consumer();
        this.producer = new Producer();
        consumer.setup(isDestinationQueue, subscribedChannel);
        producer.setup(isDestinationQueue, publishedChannel);
    }

    public Analyze() throws JMSException {
        this(true, "monitor-analyze-queue", "analyze-plan-queue");
    }

    @Override
    public void run() {
        while (true) {
            try {
                String receivedMessage = consumer.receive();
                JSONObject monData = new JSONObject(receivedMessage);
                String decidedOutput = decisionFunction(monData);
                sendMessageToJMS(decidedOutput);
            } catch (JMSException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private void sendMessageToJMS(String analyzedData) throws JMSException {
        if (analyzedData != null || !analyzedData.equals("case1")) {
            log.info(this.getClass().getSimpleName() + " send: " + analyzedData);
            producer.sendMessage(analyzedData);
        }
    }

    private double utilityFunction(String inputString){
        double w1 = 0.7;
        double w2 = 0.375;
        double v1 = 2;
        double v2 = 2;
        return w1 * v1 + w2 * v2;
    }

    private String decisionFunction(JSONObject monitoringData) {
        if (monitoringData == null || monitoringData.isEmpty()) {
            log.info("No monitoring data received");
            return null;
        }
        log.info("Received monitoring data: " + monitoringData.toString());

        String outputString = "";

        double u = utilityFunction(monitoringData.toString());
        if(u <= 0){
            outputString = "case1";
        } else if (u >= 0 && u <= 0.5) {
            outputString = "case2";
        } else if (u >= 0.5 && u <= 1) {
            outputString = "case3";
        } else {
            outputString = "case4";
        }
        return outputString;
    }
}