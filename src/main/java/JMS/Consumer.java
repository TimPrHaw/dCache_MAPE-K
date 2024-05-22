package JMS;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.*;
import java.util.logging.Logger;

/**
 * This class represents a JMS Consumer.
 */
public class Consumer {
    private static final Logger log = Logger.getLogger(Consumer.class.getName());
    private static final int DEFAULT_ACKNOWLEDGE = Session.AUTO_ACKNOWLEDGE;
    private static final boolean DEFAULT_TRANSACTED = false;
    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;
    private Destination destination;
    private MessageConsumer consumer;
    private int acknowledged = DEFAULT_ACKNOWLEDGE;
    private boolean transacted = DEFAULT_TRANSACTED;
    private final String brokerURL;
    private String username;
    private String password;
    private String selector;
    private long timeout = 0;

    /**
     * Constructor with broker URL, username, and password.
     * @param brokerURL The URL of the message broker.
     * @param username The username for authentication.
     * @param password The password for authentication.
     * @throws JMSException If an error occurs during JMS operations.
     */
    public Consumer(String brokerURL, String username, String password) throws JMSException {
        this.brokerURL = brokerURL;
        this.username = username;
        this.password = password;
    }

    /**
     * Constructor with broker URL.
     * @param brokerURL The URL of the message broker.
     * @throws JMSException If an error occurs during JMS operations.
     */
    public Consumer(String brokerURL) throws JMSException {
        this.brokerURL = brokerURL;
    }

    /**
     * Default constructor using default local broker URL.
     * @throws JMSException If an error occurs during JMS operations.
     */
    public Consumer() throws JMSException {
        this(ActiveMQConnection.DEFAULT_BROKER_URL);
    }

    /**
     * Set up the consumer with the provided parameters.
     * @param isDestinationQueue True if it's a queue, false if it's a topic.
     * @param destinationName The name of the queue or topic.
     * @throws JMSException If an error occurs during JMS operations.
     */
    public void setup(Boolean isDestinationQueue, String destinationName) throws JMSException {
        setConnectionFactory(brokerURL, username, password);
        setConnection();
        setSession(this.transacted, acknowledged);
        setDestination(isDestinationQueue, destinationName);
        setMessageConsumer();
        log.info(this.getClass().getName()
                + " setup setting: Broker URL: " + brokerURL
                + " , Username: " + username
                + " , transacted: " + transacted
                + " , Acknowledged: " + acknowledged
                + " , queue bool: " + isDestinationQueue
                + " , Destination: " + destinationName);
    }

    /**
     * Set up the default consumer with the provided parameters, using a queue as destination.
     * @param destinationName The name of the queue or topic.
     * @throws JMSException If an error occurs during JMS operations.
     */
    public void setup(String destinationName) throws JMSException {
        setup(true, destinationName);
    }

    /**
     * Retrieves a message from the consumer.
     * @return The received message.
     * @throws JMSException If an error occurs during JMS operations.
     */
    public Message receiveMessage() throws JMSException {
        Message t = consumer.receive(timeout);
        //log.info(this.getClass().getName() + " received " + t.getClass().getSimpleName() + " payload: " + ((TextMessage)t).getText());
        return t;
    }

    /**
     * Retrieves a message from the consumer.
     * @param <T> The type of the message payload.
     * @return The processed message payload.
     * @throws JMSException If an error occurs during JMS operations.
     */
    public <T> T receive() throws JMSException {
        Message message = consumer.receive(timeout);
        if (message instanceof TextMessage) {
            String payload = processTextMessage(message);
            //log.info(this.getClass().getName() + " received " + message.getClass().getSimpleName() + " payload: " + payload);
            return (T) payload;
        } else if (message instanceof BytesMessage) {
            byte[] payload = processByteMessage(message);
            //log.info(this.getClass().getName() + " received " + message.getClass().getSimpleName() + " payload: " + Arrays.toString(payload));
            return (T) payload;
        } else if (message instanceof MapMessage) {
            Map<String, Object> payload = processMapMassage(message);
            //log.info(this.getClass().getName() + " received " + message.getClass().getSimpleName() + " payload: " + payload);
            return (T) payload;
        } else if (message instanceof ObjectMessage) {
            var payload = processObjectMessage(message);
            //log.info(this.getClass().getName() + " received " + message.getClass().getSimpleName() + " payload: Object" );
            return (T) payload;
        } else if (message instanceof StreamMessage) {
            var payload = processStreamMessage(message);
            //log.info(this.getClass().getName() + " received " + message.getClass().getSimpleName() + " payload: " + Arrays.toString(payload));
            return (T) payload;
        }
        //log.info(this.getClass().getName() + " received " + message.getClass().getSimpleName() + " payload: null");
        return (T) message;
    }

    /**
     * Closes the JMS resources.
     * @throws JMSException If an error occurs during JMS operations.
     */
    public void close() throws JMSException {
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }
        if (session != null){
            session.close();
            session = null;
        }
        if (connection != null){
            connection.close();
            connection = null;
        }
    }

    /**
     * Sets the acknowledgement mode for the session.
     * @param acknowledged The acknowledgement mode to be set.
     */
    public void setAcknowledged(int acknowledged) {
        this.acknowledged = acknowledged;
    }

    /**
     * Sets whether the session is transacted or not.
     * @param transacted True if the session is transacted, false otherwise.
     */
    public void setTransacted(boolean transacted) {
        this.transacted = transacted;
    }

    /**
     * Sets the message selector for the consumer.
     * @param selector The message selector to be set.
     */
    public void setSelector(String selector) {
        this.selector = selector;
    }

    /**
     * Sets the timeout for receiving messages.
     * @param timeout The timeout value in milliseconds.
     */
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    private String processTextMessage(Message message) throws JMSException {
        return ((TextMessage) message).getText();
    }

    private byte[] processByteMessage(Message message) throws JMSException{
        byte[] payload = new byte[(int) ((BytesMessage) message).getBodyLength()];
        ((BytesMessage) message).readBytes(payload);
        return payload;
    }

    private Map<String, Object> processMapMassage(Message message) throws JMSException {
        Map<String, Object> map = new HashMap<>();
        Enumeration<String> keys = ((MapMessage) message).getMapNames();
        while (keys.hasMoreElements()){
            String key = keys.nextElement();
            map.put(key, ((MapMessage) message).getObject(key));
        }
        return map;
    }

    private Object processObjectMessage(Message message) throws JMSException{
        return ((ObjectMessage)message);
    }

    private Object[] processStreamMessage(Message message) throws JMSException {
        StreamMessage streamMessage = (StreamMessage) message;
        ArrayList<Object> payload = new ArrayList<>();
        while (true){
            try{
                payload.add(streamMessage.readObject());
            }
            catch (Exception e){
                break;
            }
        }
        return payload.toArray();
    }

    private void setConnectionFactory(String brokerURL, String username, String password){
        if (username != null && password != null){
            connectionFactory = new ActiveMQConnectionFactory(username, password, brokerURL);
        } else {
            connectionFactory = new ActiveMQConnectionFactory(brokerURL);
        }
        ((ActiveMQConnectionFactory)connectionFactory).setTrustAllPackages(true);
        ((ActiveMQConnectionFactory)connectionFactory).setMessagePrioritySupported(true);
        // TODO: Hier muss vielleicht noch etwas hin
    }

    private void setConnection() throws JMSException {
        connection = connectionFactory.createConnection();
        connection.start();
    }

    private void setSession(boolean transacted, int acknowledged) throws JMSException {
        session = connection.createSession(transacted, acknowledged);
    }

    private void setDestination(boolean isItAQueue, String destinationName) throws JMSException {
        if(isItAQueue){
            destination = session.createQueue(destinationName);
        } else {
            destination = session.createTopic(destinationName);
        }
    }
    private void setMessageConsumer() throws JMSException {
        if (selector == null) {
            consumer = session.createConsumer(destination);
        } else {
            consumer = session.createConsumer(destination, selector);
        }
    }
}