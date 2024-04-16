package JMS.consumer;

import JMS.Producer;
import JMS.Consumer;
import JMS.testClass.TestObjectClass;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.jms.*;
import java.util.HashMap;
import java.util.Map;

public class ConsumerTest {
    private String queueName = "test-consumer-queue";
    private Consumer consumer;
    private Producer producer;

    /**
     * Sets up the necessary resources for testing.
     * @throws Exception If an error occurs during setup.
     */
    @Before
    public void setUp() throws Exception {
        consumer = new Consumer();
        producer = new Producer();
    }

    /**
     * Cleans up the resources after testing.
     * @throws Exception If an error occurs during teardown.
     */
    @After
    public void tearDown() throws Exception {
        consumer.close();
        producer.close();
    }

    /**
     * Test consuming a text message.
     * @throws JMSException If an error occurs during JMS operations.
     */
    @Test
    public void setupQueue_ConsumeTextMessage_runGetMessage() throws JMSException {
        String testMessage = "Hello World";
        consumer.setup(true, queueName);
        producer.setup(true, queueName);

        producer.sendMessage(testMessage);
        Message test = consumer.runGetMessage();
        TextMessage textMessage = (TextMessage) test;

        Assert.assertNotNull(test);
        Assert.assertEquals(testMessage, textMessage.getText());
    }

    /**
     * Test consuming a byte message.
     * @throws JMSException If an error occurs during JMS operations.
     */
    @Test
    public void setupQueue_ConsumeByteMessage() throws JMSException {
        byte[] testInput = {10,20,30,40,50};

        consumer.setup(true, queueName);
        producer.setup(true, queueName);

        producer.sendMessage(testInput);
        byte[] test = consumer.run();

        Assert.assertNotNull(test);
        Assert.assertArrayEquals(testInput, test);
    }

    /**
     * Test consuming a map message.
     * @throws JMSException If an error occurs during JMS operations.
     */
    @Test
    public void setupQueue_ConsumeMapMessage() throws JMSException {
        Map<String, Object> testInput = new HashMap<>();
        testInput.put("key_1", (byte) 1);
        testInput.put("key_2", (short) 2);
        testInput.put("key_3", (char) 'a');
        testInput.put("key_4", (int) 4);
        testInput.put("key_5", (long) 5);
        testInput.put("key_6", (Float) 6.1f);
        testInput.put("key_7", (Double) 7.7);
        testInput.put("key_8", (String) "value008");
        testInput.put("key_10", true);

        consumer.setup(true, queueName);
        producer.setup(true, queueName);

        producer.sendMessage(testInput);
        Map<String, Object> test = consumer.run();

        Assert.assertNotNull(test);
        Assert.assertEquals(testInput, test);
    }

    /**
     * Test consuming a text message.
     * @throws JMSException If an error occurs during JMS operations.
     */
    @Test
    public void setupQueue_ConsumeTextMessage() throws JMSException {
        String testMessage = "Hello World";

        consumer.setup(true, queueName);
        producer.setup(true, queueName);

        producer.sendMessage(testMessage);
        String test = consumer.run();

        Assert.assertNotNull(test);
        Assert.assertEquals(testMessage, test);
    }

    /**
     * Test consuming an object message.
     * @throws JMSException If an error occurs during JMS operations.
     */
    @Test
    public void setupQueue_ConsumeObjectMessage() throws JMSException {
        TestObjectClass testObjectClass = new TestObjectClass(1, "TestName1");

        consumer.setup(true, queueName);
        producer.setup(true, queueName);

        producer.sendMessage(testObjectClass);
        ObjectMessage test = consumer.run();

        TestObjectClass testObjectClassConsume = (TestObjectClass) test.getObject();

        Assert.assertNotNull(test);
        Assert.assertEquals(testObjectClass.toString(), testObjectClassConsume.toString());
    }

    /**
     * Test consuming a stream message.
     * @throws JMSException If an error occurs during JMS operations.
     */
    @Test
    public void setupQueue_ConsumeStreamMessage() throws JMSException {
        Object[] testInput = {12, "StreamTest", '2'};

        consumer.setup(true, queueName);
        producer.setup(true, queueName);

        producer.sendMessage(testInput);
        Object[] test = consumer.run();

        Assert.assertNotNull(test);
        Assert.assertArrayEquals(testInput, test);
    }
}
