package phases.plan;

import JMS.Producer;
import JMS.Consumer;

import javax.jms.JMSException;
import java.util.Objects;
import java.util.logging.Logger;


public class Plan implements Runnable{
    private static final Logger log = Logger.getLogger(Plan.class.getName());
    private static final String DEFAULT_SUBSCRIBED_CHANNEL = "analyze-plan-queue";
    private static final String DEFAULT_PUBLISHED_CHANNEL = "plan-execute-queue";
    private Consumer consumer = null;
    private Producer producer = null;
    private String strategy = null;
    private String lastStrategy = null;
    private boolean inUse = false;
    private enum adaptationGoals {
        MIGRATION
    }

    /**
     * Constructs a Plan instance with specified JMS settings.
     *
     * @param isDestinationQueue indicates if the destination is a queue.
     * @param subscribedChannel the channel to subscribe to for receiving messages.
     * @param publishedChannel the channel to publish messages to.
     * @throws JMSException if there is an error in setting up JMS consumer or producer.
     */
    public Plan(boolean isDestinationQueue, String subscribedChannel, String publishedChannel) throws JMSException {
        this.consumer = new Consumer();
        this.producer = new Producer();
        consumer.setup(isDestinationQueue, subscribedChannel);
        producer.setup(isDestinationQueue, publishedChannel);
    }

    /**
     * Constructs a Plan instance with default JMS settings.
     *
     * @throws JMSException if there is an error in setting up JMS consumer or producer.
     */
    public Plan() throws JMSException {
        this(true, DEFAULT_SUBSCRIBED_CHANNEL, DEFAULT_PUBLISHED_CHANNEL);
    }

    /**
     * Continuously receives messages, determines if an adaptation is needed,
     * and triggers the executor if a new strategy is found.
     */
    @Override
    public void run(){
        try {
            while (true) {
                String messageReceived = (String)consumer.receive();
                if (!checkAnalyzedResult(messageReceived)){
                    findBestAdaptationOption(messageReceived);
                }
                if (configurationInUse()){
                    break;
                }
                triggerExecutor();
            }
        }
        catch (JMSException ex) {
            log.warning(this.getClass().getSimpleName() + ": " + ex.getMessage());
            throw new RuntimeException(ex);
        }
    }

    /**
     * Checks the analyzed result if a warning strategy is needed.
     *
     * @param messageReceived the message received from the analyze-phase.
     * @return true if a warning strategy is set, otherwise false.
     */
    private boolean checkAnalyzedResult(String messageReceived){
        if (messageReceived.equals("warning")){
            strategy = adaptationGoals.MIGRATION.name();
            return true;}
        return false;
    }

    /**
     * Find the best strategy based on the received message.
     *
     * @param messageReceived the message received from the analyze-phase.
     */
    private void findBestAdaptationOption(String messageReceived){
        switch (messageReceived) {
            case "case1":
                strategy = adaptationGoals.MIGRATION.name();
                break;
            case "case2":
                strategy = adaptationGoals.MIGRATION.name();
                break;
            case "case3":
                strategy = adaptationGoals.MIGRATION.name();
                break;
            default:
                strategy = null;
        }
    }

    /**
     * Checks if the current configuration is already in use.
     * TODO: Current problem: If the planing-phase set a strategy, it will be stuck because we only have one strategy.
     * TODO: Possible changes are: Add more strategies; Add a timer...
     * @return true if the configuration is in use or the strategy is null, otherwise false.
     */
    private boolean configurationInUse(){
        if (Objects.equals(lastStrategy, strategy) || strategy == null){
            return true;
        }
        inUse = false;
        return false;
    }

    /**
     * Sending the strategy to the execute-phase.
     *
     * @throws JMSException if there is an error in sending the message.
     */
    private void triggerExecutor() throws JMSException {
        log.info(this.getClass().getName() + " sending message " + strategy);
        producer.sendMessage(strategy);
        inUse = true;
        lastStrategy = strategy;
    }
}
