package phases.analyze;

import JMS.Producer;
import JMS.Consumer;
import org.json.JSONObject;

import javax.jms.JMSException;
import java.util.logging.Logger;

public class Analyze implements Runnable{
    private static final Logger log = Logger.getLogger(Analyze.class.getName());
    private static final int CRITICAL_WARNING_THRESHOLD = 330;  //TODO: change to whatever is reasonable
    private static final int MINIMAL_UTILITY_THRESHOLD = 0;//500; //TODO: change to whatever is reasonable
    private static final int MEDIUM_UTILITY_THRESHOLD = 5;//550; //TODO: change to whatever is reasonable
    private static final int MAXIMAL_UTILITY_THRESHOLD = 10;//600; //TODO: change to whatever is reasonable
    private static final double NUM_ERR_LOG_ENTRIES_WEIGHT = 0.2;
    private static final double MEDIA_ERRORS_WEIGHT = 0.25;
    private static final double CRITICAL_WARNING_WEIGHT = 0.7;
    private static final String DEFAULT_SUBSCRIBED_CHANNEL = "monitor-analyze-queue";
    private static final String DEFAULT_PUBLISHED_CHANNEL = "analyze-plan-queue";
    private Consumer consumer = null;
    private Producer producer = null;
    private int criticalWarning = 0;
    private int numErrLogEntries = 0;
    private int mediaErrors = 0;
    private int lastCriticalWarning = 0;
    private int lastNumErrLogEntries = 0;
    private int lastMediaErrors = 0;


    /**
     * Constructs an Analyze-instance with specified JMS settings.
     *
     * @param isDestinationQueue indicates if the destination is a queue.
     * @param subscribedChannel the channel to subscribe to for receiving messages.
     * @param publishedChannel the channel to publish messages to.
     * @throws JMSException if there is an error in setting up JMS consumer or producer.
     */
    public Analyze(boolean isDestinationQueue, String subscribedChannel, String publishedChannel) throws JMSException {
        this.consumer = new Consumer();
        this.producer = new Producer();
        consumer.setup(isDestinationQueue, subscribedChannel);
        producer.setup(isDestinationQueue, publishedChannel);
    }

    /**
     * Constructs an Analyze-instance with default JMS settings.
     *
     * @throws JMSException if there is an error in setting up JMS consumer or producer.
     */
    public Analyze() throws JMSException {
        this(true, DEFAULT_SUBSCRIBED_CHANNEL, DEFAULT_PUBLISHED_CHANNEL);
    }

    /**
     * Continuously receives messages, checks if adaptation is required,
     * and triggers the planner if needed.
     */
    @Override
    public void run() {
        while (true) {
            try {
                String receivedMessage = consumer.receive();
                JSONObject monitorData = new JSONObject(receivedMessage);
                if (checkMonitorData(monitorData)) {
                    setCurrentMonitorData(monitorData);
                    if (isAdaptationRequired()) {
                        String decidedOutput = decisionFunction();
                        triggerPlanner(decidedOutput);
                    }
                }
            } catch (JMSException ex) {
                log.warning(this.getClass().getSimpleName() + ": " + ex.getMessage());
                throw new RuntimeException(ex);
            }
        }
    }

    /**
     * Checks if the monitoring data is valid.
     *
     * @param monitorData the JSONObject containing the monitoring data.
     * @return true if the monitoring data is valid, otherwise false.
     */
    private boolean checkMonitorData(JSONObject monitorData) {
        if (monitorData == null || monitorData.isEmpty()) {
            log.info(this.getClass().getSimpleName() + " : No monitoring data received");
            return false;
        }
        log.info(this.getClass().getSimpleName() + " : Received monitoring data: " + monitorData);
        return true;
    }

    /**
     * Sets the current monitor data values.
     *
     * @param monitorData the JSONObject containing the monitoring data.
     */
    private void setCurrentMonitorData(JSONObject monitorData) {
        this.criticalWarning = monitorData.optInt("critical_warning", lastCriticalWarning);
        this.numErrLogEntries = monitorData.optInt("num_err_log_entries", lastNumErrLogEntries);
        this.mediaErrors = monitorData.optInt("media_errors", lastMediaErrors);
    }

    /**
     * Checks if adaptation is required based on changes in monitoring data.
     *
     * @return true if any monitored value has changed since the last check, otherwise false.
     */
    private boolean isAdaptationRequired(){
        return this.lastCriticalWarning != this.criticalWarning ||
                this.lastNumErrLogEntries != this.numErrLogEntries ||
                this.lastMediaErrors != this.mediaErrors;
    }

    /**
     * Determines the necessary action based on the received monitoring data.
     *
     * @return a string representing the decided action.
     */
    private String decisionFunction() {
        if (criticalWarning >= CRITICAL_WARNING_THRESHOLD){
            return "warning";
        }
        double u = utilityFunction(numErrLogEntries, mediaErrors, criticalWarning);
        if(u <= MINIMAL_UTILITY_THRESHOLD){
            return "fine";
        } else if (u > MINIMAL_UTILITY_THRESHOLD && u <= MEDIUM_UTILITY_THRESHOLD) {
            return "case1";
        } else if (u > MEDIUM_UTILITY_THRESHOLD && u <= MAXIMAL_UTILITY_THRESHOLD) {
            return "case2";
        }
        return "case3";
    }

    /**
     * Calculates the utility value based on weighted error metrics.
     *
     * @param num_err_log_entries number of error log entries.
     * @param media_errors number of media errors.
     * @param critical_warning number of critical warnings.
     * @return the calculated utility value.
     */
    private double utilityFunction(int num_err_log_entries, int media_errors, int critical_warning){
        return num_err_log_entries * NUM_ERR_LOG_ENTRIES_WEIGHT
                + media_errors * MEDIA_ERRORS_WEIGHT
                + critical_warning * CRITICAL_WARNING_WEIGHT;
    }

    /**
     * Sends the planner the analyzed data if required.
     *
     * @param analyzedData the data to be sent to the planner.
     * @throws JMSException if there is an error in sending the message.
     */
    private void triggerPlanner(String analyzedData) throws JMSException{
        if (analyzedData != null || !analyzedData.equals("fine")) {
            log.info(this.getClass().getSimpleName() + " send: " + analyzedData);
            producer.sendMessage(analyzedData);
            lastCriticalWarning = criticalWarning;
            lastNumErrLogEntries = numErrLogEntries;
            lastMediaErrors = mediaErrors;
        }
    }
}