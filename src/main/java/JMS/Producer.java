package JMS;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import javax.jms.IllegalStateException;
import java.io.Serializable;
import java.util.Map;
import java.util.logging.Logger;

/**
 * This class represents a JMS Producer.
 */
public class Producer {
    private static final Logger log = Logger.getLogger(Producer.class.getName());
    private static final int DEFAULT_ACKNOWLEDGE = Session.AUTO_ACKNOWLEDGE;
    private static final boolean DEFAULT_TRANSACTED = false;
    private static final boolean QUEUE = true;
    private boolean transacted = DEFAULT_TRANSACTED;
    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;
    private Destination destination;
    private MessageProducer producer;
    private int acknowledged = DEFAULT_ACKNOWLEDGE;
    private String brokerURL;
    private String username;
    private String password;

    /**
     * Constructor with broker URL, username, and password.
     * @param brokerURL The URL of the message broker.
     * @param username The username for authentication.
     * @param password The password for authentication.
     * @throws JMSException If an error occurs during JMS operations.
     */
    public Producer(String brokerURL, String username, String password) throws JMSException {
        this.brokerURL = brokerURL;
        this.username = username;
        this.password = password;
    }

    /**
     * Constructor with broker URL.
     * @param brokerURL The URL of the message broker.
     * @throws JMSException If an error occurs during JMS operations.
     */
    public Producer(String brokerURL) throws JMSException {
        this.brokerURL = brokerURL;
    }

    /**
     * Default constructor using default local broker URL.
     * @throws JMSException If an error occurs during JMS operations.
     */
    public Producer() throws JMSException {
        this(ActiveMQConnection.DEFAULT_BROKER_URL);
    }

    /**
     * Set up the producer with the provided parameters.
     * @param isDestinationQueue True if it's a queue, false if it's a topic.
     * @param destinationName The name of the queue or topic.
     * @throws JMSException If an error occurs during JMS operations.
     */
    public void setup(boolean isDestinationQueue, String destinationName) throws JMSException {
        setConnectionFactory(brokerURL, username, password);
        setConnection();
        setSession(this.transacted, acknowledged);
        setDestination(isDestinationQueue, destinationName);
        setMessageProducer();
        log.info(this.getClass().getName()
                + " setup setting: Broker URL: " + brokerURL
                + " , Username: " + username
                + " , transacted: " + transacted
                + " , Acknowledged: " + acknowledged
                + " , queue bool: " + isDestinationQueue
                + " , Destination: " + destinationName);
    }

    /**
     * Set up the default producer with the provided parameters, using a queue as the destination..
     * @param destinationName The name of the queue.
     * @throws JMSException If an error occurs during JMS operations.
     */
    public void setup(String destinationName) throws JMSException {
        setup(true, destinationName);
    }

    /**
     * Sets whether the session is transacted or not
     * @param transacted True if the session is transacted, false otherwise
     */
    public void setTransacted(boolean transacted) {
        this.transacted = transacted;
    }

    /**
     * Sends a message.
     * @param payload The payload of the message.
     * @throws JMSException If an error occurs during JMS operations.
     * @throws IllegalStateException If the payload type is not recognized.
     */
    public void sendMessage(Object payload) throws JMSException {
        sendMessage(payload, null,null );
    }

    /**
     * Sends a prioritized message.
     * @param payload The payload of the message.
     * @param priority The priority level of the message, where the lowest = 0, the default = 4 and the highest = 9
     * @throws JMSException If an error occurs during JMS operations.
     * @throws IllegalStateException If the payload type is not recognized.
     */
    public void sendMessage(Object payload, int priority) throws JMSException {
        try {
            producer.setPriority(priority);
            sendMessage(payload);
            producer.setPriority(Message.DEFAULT_PRIORITY);
        }
        catch (IllegalStateException e) {
            log.warning(e.getMessage());
        }
    }

    /**
     * Sends a message with properties.
     * @param payload The payload of the message.
     * @param name The name of the property.
     * @param value The Java object property value to set, it works only for the primitive object types (Integer, Double, Long ...) and String objects.
     * @throws JMSException If an error occurs during JMS operations.
     * @throws IllegalStateException If the payload type is not recognized.
     */
    public void sendMessage(Object payload, String name, Object value) throws JMSException {
        Message message;
        if (payload instanceof byte[]) {
            message = setByteMessage((byte[]) payload);
        } else if (payload instanceof Map<?,?>) {
            message = setMapMessage((Map<String, Object>) payload);
        } else if (payload instanceof Object[]) {
            message = setStreamMessage((Object[]) payload);
        } else if (payload instanceof String) {
            message = setTextMessage((String) payload);
        } else if (payload instanceof Serializable) {
            message = setObjectMessage(payload);
        } else {
            throw new IllegalStateException("Unknown DataType: " + payload.getClass());
        }
        if (name != null && value != null) {
            try {
                message.setObjectProperty(name, value);
            } catch (MessageFormatException e) {
                log.warning(e.getMessage());
            }
        }
        producer.send(destination, message);
    }


    /**
     * Closes the resources.
     * @throws JMSException If an error occurs during JMS operations.
     */
    public void close() throws JMSException {
        if (producer != null) {
            producer.close();
            producer = null;
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
     * Commits the messages if the transacted flag is set, otherwise nothing happens.
     * @throws JMSException If an error occurs during JMS operations.
     */
    public void commitMessages() throws JMSException {
        if (this.transacted){
            session.commit();
        }
    }

    private void setConnectionFactory(String brokerURL, String username, String password){
        if (username != null && password != null){
            connectionFactory = new ActiveMQConnectionFactory(username, password, brokerURL);
        } else {
            connectionFactory = new ActiveMQConnectionFactory(brokerURL);
        }
        ((ActiveMQConnectionFactory)connectionFactory).setTrustAllPackages(true);
        ((ActiveMQConnectionFactory)connectionFactory).setMessagePrioritySupported(true);
        // TODO: Hier muss vielleicht noch was hin
    }

    private void setConnection() throws JMSException {
        connection = connectionFactory.createConnection();
        connection.start();
    }

    private void setSession(boolean transacted, int acknowledged) throws JMSException {
        session = connection.createSession(transacted, acknowledged);
    }

    private void setDestination(boolean queueBool, String destinationName) throws JMSException {
        if(queueBool){
            destination = session.createQueue(destinationName);
        } else {
            destination = session.createTopic(destinationName);
        }
    }

    private void setMessageProducer() throws JMSException {
        producer = session.createProducer(destination);
    }

    private BytesMessage setByteMessage(byte[] bytes) throws JMSException {
        BytesMessage bytesMessage = session.createBytesMessage();
        bytesMessage.writeBytes(bytes);
        return bytesMessage;
    }

    private MapMessage setMapMessage(Map<String, Object> payload) throws JMSException {
        MapMessage mapMessage = session.createMapMessage();

        for (Map.Entry<String, Object> entry : payload.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Boolean) {
                mapMessage.setBoolean(key, (Boolean) value);
            } else if (value instanceof Byte) {
                mapMessage.setByte(key, (Byte) value);
            } else if (value instanceof Short) {
                mapMessage.setShort(key, (Short) value);
            } else if (value instanceof Character) {
                mapMessage.setChar(key, (Character) value);
            } else if (value instanceof Integer) {
                mapMessage.setInt(key, (Integer) value);
            } else if (value instanceof Long) {
                mapMessage.setLong(key, (Long) value);
            } else if (value instanceof Float) {
                mapMessage.setFloat(key, (Float) value);
            } else if (value instanceof Double) {
                mapMessage.setDouble(key, (Double) value);
            } else if (value instanceof String) {
                mapMessage.setString(key, (String) value);
            } else if (value instanceof byte[]) {
                mapMessage.setBytes(key, (byte[]) value);
            }
        }
        return mapMessage;
    }

    private ObjectMessage setObjectMessage(Object payload) throws JMSException {
        ObjectMessage objectMessage = session.createObjectMessage();
        objectMessage.setObject((Serializable) payload);
        return objectMessage;
    }

    private StreamMessage setStreamMessage(Object[] objects) throws JMSException {
        StreamMessage streamMessage = session.createStreamMessage();

        for(Object object : objects) {
            if (object instanceof Boolean) {
                streamMessage.writeBoolean((Boolean) object);
            } else if (object instanceof Byte) {
                streamMessage.writeByte((Byte) object);
            } else if (object instanceof Short) {
                streamMessage.writeShort((Short) object);
            } else if (object instanceof Character) {
                streamMessage.writeChar((Character) object);
            } else if (object instanceof Integer) {
                streamMessage.writeInt((Integer) object);
            } else if (object instanceof Long) {
                streamMessage.writeLong((Long) object);
            } else if (object instanceof Float) {
                streamMessage.writeFloat((Float) object);
            } else if (object instanceof Double) {
                streamMessage.writeDouble((Double) object);
            } else if (object instanceof String) {
                streamMessage.writeString((String) object);
            } else if (object instanceof byte[]) {
                streamMessage.writeBytes((byte[]) object);
            }
        }
        return streamMessage;
    }

    private TextMessage setTextMessage(String payload) throws JMSException {
        TextMessage textMessage = session.createTextMessage(payload);
        return textMessage;
    }
}
