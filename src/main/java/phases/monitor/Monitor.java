package phases.monitor;

import JMS.Producer;
import org.json.JSONException;
import org.json.JSONObject;

import javax.jms.JMSException;
import java.util.logging.Logger;

public class Monitor implements Runnable {
    private static final Logger log = Logger.getLogger(Monitor.class.getName());
    private final MessageReceiver messageReceiver;
    private final Producer producer;
    private final Integer THREAD_SLEEP_TIME = 5000;
    private static final String DEFAULT_PUBLISHED_CHANNEL = "monitor-analyze-queue";


    /**
     * Constructs a Monitor-instance with specified message receiver settings and a specified JMS published channel.
     *
     * @param messageReceiver the type of message receiver to use.
     * @param topic the topic to subscribe to.
     * @param bootstrapServers the bootstrap servers for the message receiver.
     * @param publishedChannel the channel to publish messages to.
     * @throws JMSException if there is an error in setting up the JMS producer.
     */
    public Monitor(String messageReceiver, String topic, String bootstrapServers, String publishedChannel) throws JMSException {
        producer = new Producer();
        producer.setup(publishedChannel);
        this.messageReceiver = MessageReceiverFactory.createMessageReceiver(messageReceiver, topic, bootstrapServers);
        log.info(this.getClass().getName() + " built messageReceiver: " + this.messageReceiver.toString());
    }

    /**
     * Constructs a Monitor instance with specified message receiver settings and the default JMS published channel.
     *
     * @param messageReceiver the type of message receiver to use.
     * @param topic the topic to subscribe to.
     * @param bootstrapServers the bootstrap servers for the message receiver.
     * @throws JMSException if there is an error in setting up the JMS producer.
     */
    public Monitor(String messageReceiver, String topic, String bootstrapServers) throws JMSException {
        this(messageReceiver, topic, bootstrapServers, DEFAULT_PUBLISHED_CHANNEL);
    }

    /**
     * Constructs a Monitor instance with a specified message receiver and published channel.
     *
     * @param messageReceiver the type of message receiver to use.
     * @param publishedChannel the channel to publish messages to.
     * @throws JMSException if there is an error in setting up the JMS producer.
     */
    public Monitor(String messageReceiver, String publishedChannel) throws JMSException {
        this(messageReceiver, null, null, publishedChannel);
    }

    /**
     * Constructs a Monitor instance with a specified message receiver and the default JMS published channel.
     *
     * @param messageReceiver the type of message receiver to use.
     * @throws JMSException if there is an error in setting up the JMS producer.
     */
    public Monitor(String messageReceiver) throws JMSException {
        this(messageReceiver, null, null, DEFAULT_PUBLISHED_CHANNEL);
    }

    /**
     * Continuously receives messages from the message receiver, processes them, and sends them to the analyzer-phase.
     */
    @Override
    public void run(){
        while(true){
            try {
                JSONObject receivedJSON = messageReceiver.receiveMessage();
                sendMessageToJMS(receivedJSON);
                Thread.sleep(THREAD_SLEEP_TIME);
            } catch (NullPointerException | UnsupportedOperationException | JSONException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Sends the received monitoring data to the analyze-phase.
     *
     * @param monitoringData the JSONObject containing the monitoring data.
     */
    private void sendMessageToJMS(JSONObject monitoringData){
        try {
            producer.sendMessage(monitoringData.toString());
            log.info(this.getClass().getName() + " sent message: " + monitoringData);
        } catch (JMSException ex) {
            log.warning(this.getClass().getName() + " failed to send message: " + ex.getMessage());
            ex.printStackTrace();
        }
    }
}