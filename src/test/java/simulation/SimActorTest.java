package simulation;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.kafka.common.metrics.Sensor;
import org.junit.Assert;
import org.junit.Test;
import phases.execute.Execute;

import javax.jms.*;

public class SimActorTest {
    private String queueName1 = "actor-test-queue-in";
    private String queueName2 = "actor-test-queue-out";
    private int resetval = 20;

    @Test
    public void testReset() throws JMSException, InterruptedException {

        int textMessage = -1;

        SimActor simActor = new SimActor(true, queueName1, queueName2, resetval);

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


        testProducer.send(testPrducerSession.createObjectMessage(textMessage));

        Thread simActorThread = new Thread(() -> {
            try {
                simActor.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });
        simActorThread.start();

        Message incomingMessage = testConsumer.receive(1000);
        var msg = ((ObjectMessage) incomingMessage).getObject();
        Assert.assertEquals(20, msg);

        testProducerConnection.close();
    }

    @Test
    public void testHeating() throws JMSException, InterruptedException {

        int textMessage = 2;

        SimActor simActor = new SimActor(true, queueName1, queueName2, resetval);

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


        testProducer.send(testPrducerSession.createObjectMessage(textMessage));

        Thread simActorThread = new Thread(() -> {
            try {
                simActor.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });
        simActorThread.start();

        Message incomingMessage = testConsumer.receive(1000);
        var msg = ((ObjectMessage) incomingMessage).getObject();
        boolean val = (boolean) msg;
        Assert.assertTrue(val);

        testProducerConnection.close();
    }

    @Test
    public void testCooling() throws JMSException, InterruptedException {

        int textMessage = 3;

        SimActor simActor = new SimActor(true, queueName1, queueName2, resetval);

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


        testProducer.send(testPrducerSession.createObjectMessage(textMessage));

        Thread simActorThread = new Thread(() -> {
            try {
                simActor.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });
        simActorThread.start();

        Message incomingMessage = testConsumer.receive(1000);
        var msg = ((ObjectMessage) incomingMessage).getObject();
        boolean val = (boolean) msg;
        Assert.assertFalse(val);

        testProducerConnection.close();
    }
}
