package JMS;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import phases.analyze.Analyze;

import javax.jms.*;
import javax.jms.IllegalStateException;
import java.io.Serializable;
import java.util.Map;
import java.util.logging.Logger;


public class Producer {
    private static final Logger log = Logger.getLogger(Producer.class.getName());
    private static final int DEFAULT_ACKNOWLEDGE = Session.AUTO_ACKNOWLEDGE;
    private static final boolean DEFAULT_TRANSACTED = false;
    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;
    private Destination destination;
    private MessageProducer producer;
    private int acknowledged = DEFAULT_ACKNOWLEDGE;
    private String brokerURL;
    private String username;
    private String password;

    public Producer(String brokerURL, String username, String password) throws JMSException {
        this.brokerURL = brokerURL;
        this.username = username;
        this.password = password;
    }
    public Producer(String brokerURL) throws JMSException {
        this.brokerURL = brokerURL;
    }
    public Producer() throws JMSException {
        this(ActiveMQConnection.DEFAULT_BROKER_URL);
    }

    public void setup(boolean transacted, boolean queueBool, String destinationName) throws JMSException {
        setConnectionFactory(brokerURL, username, password);
        setConnection();
        setSession(transacted, acknowledged);
        setDestination(queueBool, destinationName);
        setMessageProducer();
        log.info(this.getClass().getName()
                + " setup setting: Broker URL: " + brokerURL
                + " , Username: " + username
                + " , transacted: " + transacted
                + " , Acknowledged: " + acknowledged
                + " , queue bool: " + queueBool
                + " , Destination: " + destinationName);
    }

    public void setup(boolean queueBool, String destinationName) throws JMSException {
        setup(DEFAULT_TRANSACTED, queueBool, destinationName);
    }

    public void sendMessage(Object payload) throws JMSException {
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
        log.info(this.getClass().getName() + " sending message: " + message);
        producer.send(destination, message);
    }
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

    public void commitSession(boolean transacted) throws JMSException {
        if (transacted){
            session.commit();
        }
    }

    private void setConnectionFactory(String brokerURL, String username, String password){
        if (username != null && password != null){
            connectionFactory = new ActiveMQConnectionFactory(username, password, brokerURL);
        } else {
            connectionFactory = new ActiveMQConnectionFactory(brokerURL);
        }
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
        ObjectMessage objectMessage = session.createObjectMessage((Serializable) payload);
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
