package phases;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Assert;
import org.junit.Test;
import phases.execute.Execute;

import javax.jms.*;

public class ExecuteTest {

    private String queueName1 = "execute-test-queue-in";
    private String queueName2 = "execute-test-queue-out";

    @Test
    public void testReset() throws JMSException {
        String textMessage = "reset";

        Execute execute= new Execute(true, queueName1, queueName2);

        // Test Producer
        Connection testProducerConnection = new ActiveMQConnectionFactory().createConnection();
        testProducerConnection.start();
        Session testPrducerSession = testProducerConnection.createSession();
        Destination testProducerDestination = testPrducerSession.createQueue(queueName1);
        MessageProducer testProducer = testPrducerSession.createProducer(testProducerDestination);

        // Test consumer
        Connection testConsumerConnection = new ActiveMQConnectionFactory().createConnection();
        testConsumerConnection.start();
        Session testConsumerSession = testConsumerConnection.createSession();
        Destination testConsumerDestination = testConsumerSession.createQueue(queueName2);
        MessageConsumer testConsumer = testConsumerSession.createConsumer(testConsumerDestination);

        testProducer.send(testPrducerSession.createTextMessage(textMessage));

        Thread analyzeThread = new Thread(() -> {
            try {
                execute.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });
        analyzeThread.start();

        Message incomingMessage = testConsumer.receive(1000);
        var msg = (double)((ObjectMessage) incomingMessage).getObject();
        Assert.assertEquals((int)msg, -1);

        testProducerConnection.close();
        testConsumerConnection.close();
    }

    @Test
    public void testToHigh() throws JMSException {
        String textMessage = "toHigh";

        Execute execute= new Execute(true, queueName1, queueName2);

        // Test Producer
        Connection testProducerConnection = new ActiveMQConnectionFactory().createConnection();
        testProducerConnection.start();
        Session testPrducerSession = testProducerConnection.createSession();
        Destination testProducerDestination = testPrducerSession.createQueue(queueName1);
        MessageProducer testProducer = testPrducerSession.createProducer(testProducerDestination);

        // Test consumer
        Connection testConsumerConnection = new ActiveMQConnectionFactory().createConnection();
        testConsumerConnection.start();
        Session testConsumerSession = testConsumerConnection.createSession();
        Destination testConsumerDestination = testConsumerSession.createQueue(queueName2);
        MessageConsumer testConsumer = testConsumerSession.createConsumer(testConsumerDestination);

        testProducer.send(testPrducerSession.createTextMessage(textMessage));

        Thread analyzeThread = new Thread(() -> {
            try {
                execute.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });
        analyzeThread.start();

        Message incomingMessage = testConsumer.receive(1000);
        var msg = (double)((ObjectMessage) incomingMessage).getObject();
        Assert.assertEquals(3, (int)msg);

        testProducerConnection.close();
        testConsumerConnection.close();
    }

    @Test
    public void testToLow() throws JMSException {
        String textMessage = "toLow";

        Execute execute= new Execute(true, queueName1, queueName2);

        // Test Producer
        Connection testProducerConnection = new ActiveMQConnectionFactory().createConnection();
        testProducerConnection.start();
        Session testPrducerSession = testProducerConnection.createSession();
        Destination testProducerDestination = testPrducerSession.createQueue(queueName1);
        MessageProducer testProducer = testPrducerSession.createProducer(testProducerDestination);

        // Test consumer
        Connection testConsumerConnection = new ActiveMQConnectionFactory().createConnection();
        testConsumerConnection.start();
        Session testConsumerSession = testConsumerConnection.createSession();
        Destination testConsumerDestination = testConsumerSession.createQueue(queueName2);
        MessageConsumer testConsumer = testConsumerSession.createConsumer(testConsumerDestination);

        testProducer.send(testPrducerSession.createTextMessage(textMessage));

        Thread analyzeThread = new Thread(() -> {
            try {
                execute.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });
        analyzeThread.start();

        Message incomingMessage = testConsumer.receive(1000);
        var msg = (double)((ObjectMessage) incomingMessage).getObject();
        Assert.assertEquals(2, (int)msg);

        testProducerConnection.close();
        testConsumerConnection.close();
    }

    @Test
    public void testOkay() throws JMSException {
        String textMessage = "okay";

        Execute execute= new Execute(true, queueName1, queueName2);

        // Test Producer
        Connection testProducerConnection = new ActiveMQConnectionFactory().createConnection();
        testProducerConnection.start();
        Session testPrducerSession = testProducerConnection.createSession();
        Destination testProducerDestination = testPrducerSession.createQueue(queueName1);
        MessageProducer testProducer = testPrducerSession.createProducer(testProducerDestination);

        // Test consumer
        Connection testConsumerConnection = new ActiveMQConnectionFactory().createConnection();
        testConsumerConnection.start();
        Session testConsumerSession = testConsumerConnection.createSession();
        Destination testConsumerDestination = testConsumerSession.createQueue(queueName2);
        MessageConsumer testConsumer = testConsumerSession.createConsumer(testConsumerDestination);

        testProducer.send(testPrducerSession.createTextMessage(textMessage));

        Thread analyzeThread = new Thread(() -> {
            try {
                execute.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });
        analyzeThread.start();

        Message incomingMessage = testConsumer.receive(1000);
        var msg = (double)((ObjectMessage) incomingMessage).getObject();
        Assert.assertEquals(1, (int)msg);

        testProducerConnection.close();
        testConsumerConnection.close();
    }

    @Test
    public void testDefault() throws JMSException {
        String textMessage = "Hello World";

        Execute execute= new Execute(true, queueName1, queueName2);

        // Test Producer
        Connection testProducerConnection = new ActiveMQConnectionFactory().createConnection();
        testProducerConnection.start();
        Session testPrducerSession = testProducerConnection.createSession();
        Destination testProducerDestination = testPrducerSession.createQueue(queueName1);
        MessageProducer testProducer = testPrducerSession.createProducer(testProducerDestination);

        // Test consumer
        Connection testConsumerConnection = new ActiveMQConnectionFactory().createConnection();
        testConsumerConnection.start();
        Session testConsumerSession = testConsumerConnection.createSession();
        Destination testConsumerDestination = testConsumerSession.createQueue(queueName2);
        MessageConsumer testConsumer = testConsumerSession.createConsumer(testConsumerDestination);

        testProducer.send(testPrducerSession.createTextMessage(textMessage));

        Thread analyzeThread = new Thread(() -> {
            try {
                execute.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });
        analyzeThread.start();

        Message incomingMessage = testConsumer.receive(1000);
        var msg = (double)((ObjectMessage) incomingMessage).getObject();
        Assert.assertEquals(0, (int)msg);

        testProducerConnection.close();
        testConsumerConnection.close();
    }
}
