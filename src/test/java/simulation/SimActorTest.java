package simulation;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.jms.*;

public class SimActorTest {
    private String queueIn = "actor-test-queue-in";
    private String queueOut = "actor-test-queue-out";
    private int resetval = 20;

    @Test
    public void testReset() throws JMSException, InterruptedException {
        int textMessage = -1;
        SimActor simActor = new SimActor(true, queueIn, queueOut, resetval);
        Session producerSession = testProducerSession();
        MessageProducer testMessageProducer = createTestMessageProducer(queueIn,producerSession);
        MessageConsumer testMessageConsumer = createTestMessageConsumer(queueOut);

        testMessageProducer.send(producerSession.createObjectMessage(textMessage));

        Thread simActorThread = new Thread(() -> {
            try {
                simActor.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });
        simActorThread.start();

        Message incomingMessage = testMessageConsumer.receive(1000);
        var msg = ((ObjectMessage) incomingMessage).getObject();
        Assert.assertEquals(20, msg);

        testMessageProducer.close();
        testMessageConsumer.close();
        producerSession.close();
    }

    @Test
    public void testHeating() throws JMSException, InterruptedException {
        int textMessage = 2;
        SimActor simActor = new SimActor(true, queueIn, queueOut, resetval);
        Session producerSession = testProducerSession();
        MessageProducer testMessageProducer = createTestMessageProducer(queueIn,producerSession);
        MessageConsumer testMessageConsumer = createTestMessageConsumer(queueOut);

        testMessageProducer.send(producerSession.createObjectMessage(textMessage));

        Thread simActorThread = new Thread(() -> {
            try {
                simActor.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });
        simActorThread.start();

        boolean msg = (Boolean)((ObjectMessage) testMessageConsumer.receive(1000)).getObject();
        Assert.assertTrue(msg);

        testMessageProducer.close();
        testMessageConsumer.close();
        producerSession.close();
    }

    @Test
    public void testCooling() throws JMSException, InterruptedException {
        int textMessage = 3;
        SimActor simActor = new SimActor(true, queueIn, queueOut, resetval);
        Session producerSession = testProducerSession();
        MessageProducer testMessageProducer = createTestMessageProducer(queueIn,producerSession);
        MessageConsumer testMessageConsumer = createTestMessageConsumer(queueOut);

        testMessageProducer.send(producerSession.createObjectMessage(textMessage));
        Thread simActorThread = new Thread(() -> {
            try {
                simActor.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });
        simActorThread.start();

        boolean msg = (Boolean)((ObjectMessage) testMessageConsumer.receive(1000)).getObject();
        Assert.assertFalse(msg);

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
