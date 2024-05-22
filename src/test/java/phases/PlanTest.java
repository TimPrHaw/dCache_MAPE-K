package phases;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Assert;
import org.junit.Test;
import phases.plan.Plan;

import javax.jms.*;

public class PlanTest {
    private String queueIn = "plan-test-queue-in";
    private String queueOut = "plan-test-queue-out";


/**
    @Test
    public void testOkay() throws JMSException {
        double textMessage = 20.2;

        Plan plan = new Plan(true, queueIn, queueOut);
        Session producerSession = testProducerSession();
        MessageProducer testMessageProducer = createTestMessageProducer(queueIn,producerSession);
        MessageConsumer testMessageConsumer = createTestMessageConsumer(queueOut);

        testMessageProducer.send(producerSession.createObjectMessage(textMessage));

        Thread analyzeThread = new Thread(() -> {
            try {
                plan.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });
        analyzeThread.start();

        Message incomingMessage = testMessageConsumer.receive(10000);
        String msg = ((TextMessage) incomingMessage).getText();
        Assert.assertEquals(msg, "okay");

        testMessageProducer.close();
        testMessageConsumer.close();
        producerSession.close();
    }
 */

    @Test
    public void testReset() throws JMSException {
        double textMessage = 100.12;

        Plan plan = new Plan(true, queueIn, queueOut);
        Session producerSession = testProducerSession();
        MessageProducer testMessageProducer = createTestMessageProducer(queueIn,producerSession);
        MessageConsumer testMessageConsumer = createTestMessageConsumer(queueOut);

        testMessageProducer.send(producerSession.createObjectMessage(textMessage));
        Thread analyzeThread = new Thread(() -> {
            plan.run();
        });
        analyzeThread.start();
        Message incomingMessage = testMessageConsumer.receive(1000);
        Assert.assertEquals("reset", ((TextMessage) incomingMessage).getText());

        testMessageProducer.close();
        testMessageConsumer.close();
        producerSession.close();
    }

    @Test
    public void testToHigh() throws JMSException {
        double textMessage = 25.2;
        Plan plan = new Plan(true, queueIn, queueOut);
        Session producerSession = testProducerSession();
        MessageProducer testMessageProducer = createTestMessageProducer(queueIn,producerSession);
        MessageConsumer testMessageConsumer = createTestMessageConsumer(queueOut);

        testMessageProducer.send(producerSession.createObjectMessage(textMessage));

        Thread analyzeThread = new Thread(() -> {
            plan.run();
        });
        analyzeThread.start();

        Message incomingMessage = testMessageConsumer.receive(1000);
        Assert.assertEquals("toHigh", ((TextMessage) incomingMessage).getText());

        testMessageProducer.close();
        testMessageConsumer.close();
        producerSession.close();
    }

    @Test
    public void testToLow() throws JMSException {
        double textMessage = 17.8;
        Plan plan = new Plan(true, queueIn, queueOut);
        Session producerSession = testProducerSession();
        MessageProducer testMessageProducer = createTestMessageProducer(queueIn,producerSession);
        MessageConsumer testMessageConsumer = createTestMessageConsumer(queueOut);

        testMessageProducer.send(producerSession.createObjectMessage(textMessage));

        Thread analyzeThread = new Thread(() -> {
            plan.run();
        });
        analyzeThread.start();

        Message incomingMessage = testMessageConsumer.receive(1000);
        Assert.assertEquals("toLow", ((TextMessage) incomingMessage).getText());

        testMessageProducer.close();
        testMessageConsumer.close();
        producerSession.close();
    }

    private Session testProducerSession() throws JMSException {
        Connection producerConnection = new ActiveMQConnectionFactory().createConnection();
        producerConnection.start();
        return producerConnection.createSession();
    }

    private MessageProducer createTestMessageProducer(String destination, Session producerSession) throws JMSException {
        Destination producerDestination = producerSession.createQueue(destination);
        return producerSession.createProducer(producerDestination);
    }

    private MessageConsumer createTestMessageConsumer(String destination) throws JMSException {
        Connection consumerConnection = new ActiveMQConnectionFactory().createConnection();
        consumerConnection.start();
        Session consumerSession = consumerConnection.createSession();
        Destination consumerDestination = consumerSession.createQueue(destination);
        return consumerSession.createConsumer(consumerDestination);
    }
}
