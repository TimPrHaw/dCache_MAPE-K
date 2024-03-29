package JMS;

import org.apache.activemq.ActiveMQConnection;
import javax.jms.*;

// TODO: ActiveMQ als Docker einbinden, zum Debugging Lokaleversion verwenden
public class SynchConsumer {
    private static final int DEFAULT_ACKNOWLEDGE = Session.AUTO_ACKNOWLEDGE;
    private static final boolean DEFAULT_TRANSACTED = false;
    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;
    private Destination destination;
    private int acknowledged;
    private boolean transacted;
    private String queueName;
    private String brokerURL;
    private String username;
    private String password;

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


    /**
    private void setConnectionFactory(String brokerURL, String username, String password){
        if (!username.isEmpty() && !password.isEmpty()){
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

    private void setSession(boolean transacted) throws JMSException {
        session = connection.createSession(transacted, DEFAULT_ACKNOWLEDGE);
    }
    private void setSession() throws JMSException {
        session = connection.createSession(DEFAULT_TRANSACTED, DEFAULT_ACKNOWLEDGE);
    }

    private void setDestination(boolean isItAQueue, String destinationName) throws JMSException {
        if(isItAQueue){
            session.createQueue(destinationName);
        } else {
            session.createTopic(destinationName);
        }
    }

     */

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