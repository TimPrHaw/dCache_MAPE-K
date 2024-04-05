package JMS;

import kafka.KafkaListener;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQMessage;

import javax.jms.*;
import java.util.logging.Logger;

// TODO: ActiveMQ als Docker einbinden, zum Debugging Lokaleversion verwenden
public class SynchConsumer {
    private static final Logger log = Logger.getLogger(SynchConsumer.class.getName());
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

    // TODO: 1. Synchronen Consumer fertig machen. 2 Asynchronen Consuumer (implements MessageListener) erstellen

    public SynchConsumer(String brokerURL, String username, String password) throws JMSException {
        this.brokerURL = brokerURL;
        this.username = username;
        this.password = password;
    }
    public SynchConsumer(String brokerURL) throws JMSException {
        this.brokerURL = brokerURL;
    }
    public SynchConsumer() throws JMSException {
        this(ActiveMQConnection.DEFAULT_BROKER_URL);
    }

    public void setup(Boolean queueBool, String queueName) throws JMSException {
        setConnectionFactory(brokerURL, username, password);
        setConnection();
        setSession(transacted, acknowledged);
        setDestination(queueBool, queueName);
        setMessageConsumer();
    }

    public Message run() throws JMSException {
        Message t = consumer.receive();
        log.info(this.getClass().getName() + " received " + t.getClass().getSimpleName() + " payload: " + ((TextMessage)t).getText());
        return t;
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

    public void setAcknowledged(int acknowledged) {
        this.acknowledged = acknowledged;
    }

    public void setTransacted(boolean transacted) {
        this.transacted = transacted;
    }

    public void setSelector(String selector) {
        this.selector = selector;
    }

    /**
    private void setMessageConsumer


    try {
        // Nachrichtenempfänger erstellen
        MessageConsumer consumer = session.createConsumer(destination);

        // Nachricht empfangen
        Message message = consumer.receive();

        if (message instanceof TextMessage) {
            TextMessage textMessage = (TextMessage) message;
            System.out.println("Nachricht empfangen: " + textMessage.getText());
        } else {
            System.out.println("Ungültige Nachricht empfangen");
        }

        // Ressourcen schließen
        consumer.close();
        session.close();
        connection.close();
    } catch (JMSException e) {
        e.printStackTrace();
    }

     */
}