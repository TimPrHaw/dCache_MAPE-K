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

    public Monitor(String messageReceiver, String topic, String bootstrapServers) throws JMSException {
        producer = new Producer();
        producer.setup("monitor-analyze-queue");
        this.messageReceiver = MessageReceiverFactory.createMessageReceiver(messageReceiver, topic, bootstrapServers);
        log.info(this.getClass().getName() + " built messageReceiver: " + this.messageReceiver.toString());
    }

    public Monitor(String messageReceiver) throws JMSException {
        this(messageReceiver, null, null);
    }

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

    private void sendMessageToJMS(JSONObject monitoringData){
        try {
            producer.sendMessage(monitoringData.toString());
        } catch (JMSException ex) {
            ex.printStackTrace();
        }
    }
}