package phases.plan;

import JMS.Producer;
import JMS.Consumer;

import javax.jms.JMSException;
import javax.jms.*;
import java.util.Objects;
import java.util.logging.Logger;


public class Plan implements Runnable{
    private static final Logger log = Logger.getLogger(Plan.class.getName());
    private Consumer consumer = null;
    private Producer producer = null;
    private String strategy = null;
    private String lastStrategy = null;
    private boolean inUse = false;
    private enum adaptationGoals {
        MIGRATION
    }

    public Plan(boolean isDestinationQueue, String subscribedChannel, String publishedChannel) throws JMSException {
        this.consumer = new Consumer();
        this.producer = new Producer();
        consumer.setup(isDestinationQueue, subscribedChannel);
        producer.setup(isDestinationQueue, publishedChannel);
    }

    public Plan() throws JMSException {
        this(true, "analyze-plan-queue", "plan-execute-queue");
    }

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
        catch (JMSException ex) {}
    }

    private boolean checkAnalyzedResult(String messageReceived){
        if (messageReceived.equals("warning")){
            strategy = adaptationGoals.MIGRATION.name();
            return true;}
        return false;
    }

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

    private boolean configurationInUse(){
        if (Objects.equals(lastStrategy, strategy) || strategy == null){
            return true;
        }
        inUse = false;
        return false;
    }

    private void triggerExecutor() throws JMSException {
        log.info(this.getClass().getName() + " sending message " + strategy);
        producer.sendMessage(strategy);
        inUse = true;
        lastStrategy = strategy;
    }
}
