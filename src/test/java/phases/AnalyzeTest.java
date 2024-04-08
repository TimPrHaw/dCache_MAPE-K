package phases;

import org.apache.activemq.*;

import javax.jms.*;
import javax.jms.Message;

import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import org.junit.Assert;
import org.junit.Test;
import phases.analyze.Analyze;


public class AnalyzeTest {

    private String queueName1 = "analyze-test-queue-in";
    private String queueName2 = "analyze-test-queue-out";

    @Test
    public void test() throws JMSException {
        String textMessage = "[1, 2, 3, 4, 5, 6, 7, 8, 9]";

        Analyze analyze = new Analyze(true, queueName1, queueName2);

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
                analyze.run();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });
        analyzeThread.start();

        Message incomingMessage = testConsumer.receive(1000);
        Object msg = ((ObjectMessage) incomingMessage).getObject();
        Assert.assertEquals(msg, 5.0);

        testProducerConnection.close();
        testConsumerConnection.close();

    }
}
