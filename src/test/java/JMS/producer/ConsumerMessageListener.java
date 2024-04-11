package JMS.producer;

import javax.jms.Message;
import javax.jms.MessageListener;

public class ConsumerMessageListener implements MessageListener {
    private String consumerName;
    private Message lastMessage;
    public ConsumerMessageListener(String consumerName) {
        this.consumerName = consumerName;
    }

    public void onMessage(Message message) {
        System.out.println("New message received at: " + this.consumerName);
        this.lastMessage = message;
        }
    public Message getLastMessage() {
        return lastMessage;
    }
}
