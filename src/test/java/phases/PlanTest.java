package phases;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Assert;
import org.junit.Test;
import phases.plan.Plan;

import javax.jms.*;

public class PlanTest {

    private String queueName1 = "plan-test-queue-in";
    private String queueName2 = "plan-test-queue-out";

    @Test
    public void testOkay() throws JMSException {
        String textMessage = "20.2";

        Plan plan = new Plan(true, queueName1, queueName2);

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
                plan.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });
        analyzeThread.start();

        Message incomingMessage = testConsumer.receive(1000);
        String msg = ((TextMessage) incomingMessage).getText();
        Assert.assertEquals(msg, "okay");

        testProducerConnection.close();
        testConsumerConnection.close();
    }

    @Test
    public void testReset() throws JMSException {
        String textMessage = "100.12";

        Plan plan = new Plan(true, queueName1, queueName2);

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
                plan.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });
        analyzeThread.start();

        Message incomingMessage = testConsumer.receive(1000);
        String msg = ((TextMessage) incomingMessage).getText();
        Assert.assertEquals("reset", msg);

        testProducerConnection.close();
        testConsumerConnection.close();
    }

    @Test
    public void testToHigh() throws JMSException {
        String textMessage = "25.2";

        Plan plan = new Plan(true, queueName1, queueName2);

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
                plan.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });
        analyzeThread.start();

        Message incomingMessage = testConsumer.receive(1000);
        String msg = ((TextMessage) incomingMessage).getText();
        Assert.assertEquals("toHigh", msg);

        testProducerConnection.close();
        testConsumerConnection.close();
    }

    @Test
    public void testToLow() throws JMSException {
        String textMessage = "17.8";

        Plan plan = new Plan(true, queueName1, queueName2);

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
                plan.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });
        analyzeThread.start();

        Message incomingMessage = testConsumer.receive(1000);
        String msg = ((TextMessage) incomingMessage).getText();
        Assert.assertEquals("toLow", msg);

        testProducerConnection.close();
        testConsumerConnection.close();
    }
}
