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
    private JSONObject monitorData = null;
    private int lastCriticalWarning = 0;
    private int lastNumErrLogEntries = 0;
    private int lastMediaErrors = 0;


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
                this.monitorData = new JSONObject(receivedMessage);
                if (isAdaptationRequired()) {
                    String decidedOutput = decisionFunction();
                    triggerPlanner(decidedOutput);
                }
            } catch (JMSException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private boolean isAdaptationRequired(){
        return this.lastCriticalWarning != monitorData.getInt("critical_warning") || this.lastNumErrLogEntries != monitorData.getInt("num_err_log_entries") || this.lastMediaErrors != monitorData.getInt("media_errors");
    }

    private String decisionFunction() {
        if (monitorData == null || monitorData.isEmpty()) {
            log.info("No monitoring data received");
            return null;
        }
        log.info("Received monitoring data: " + monitorData.toString());

        if (monitorData.getInt("critical_warning") >= 330){
            return "warning";
        }

        double u = utilityFunction(
                monitorData.getInt("num_err_log_entries"),
                monitorData.getInt("media_errors"),
                monitorData.getInt("critical_warning"));

        if(u <= 500){
            return "fine";
        } else if (u > 500 && u <= 550) { // TODO: final static int
            return "case1";
        } else if (u > 550 && u <= 600) {
            return "case2";
        }
        return "case3";
    }

    private double utilityFunction(int num_err_log_entries, int media_errors, int critical_warning){
        double w1 = 0.2;
        double w2 = 0.25;
        double w3 = 0.7;
        return num_err_log_entries * w1 + media_errors * w2 + critical_warning * w3;
    }

    private void triggerPlanner(String analyzedData) throws JMSException {
        if (analyzedData != null || !analyzedData.equals("fine")) {
            log.info(this.getClass().getSimpleName() + " send: " + analyzedData);
            producer.sendMessage(analyzedData);
            lastCriticalWarning = monitorData.getInt("critical_warning");
            lastNumErrLogEntries = monitorData.getInt("num_err_log_entries");
            lastMediaErrors = monitorData.getInt("media_errors");
        }
    }
}