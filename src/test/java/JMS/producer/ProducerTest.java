package JMS.producer;

import javax.jms.JMSException;

import JMS.Producer;
import JMS.testClass.TestObjectClass;
import org.apache.activemq.*;

import javax.jms.*;
import javax.jms.Message;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

public class ProducerTest {
    private final String queueName = "test-queue";
    private final String topicName = "test/topic";
    private Producer testProducer;
    private MessageConsumer testMessageConsumerQueue;

    /**
     * Sets up the necessary resources for testing.
     * @throws Exception If an error occurs during setup.
     */
    @Before
    public void setUp() throws Exception {
        testProducer = new Producer();
        testMessageConsumerQueue = createTestConsumer(queueName);
         }

    /**
     * Cleans up the resources after testing.
     * @throws Exception If an error occurs during teardown.
     */
    @After
    public void tearDown() throws Exception {
        testProducer.setTransacted(false);
        testProducer.close();
        testMessageConsumerQueue.close();
    }

    /**
     * Test sending a short value and consuming the message.
     * @throws JMSException If an error occurs during JMS operations.
     */
    @Test
    public void testSendMessage_setupNoTransactionQueue_SendShort_thenConsumeMessage() throws JMSException{
        short testInput = Short.MIN_VALUE;
        testProducer.setup(true, queueName);
        testProducer.sendMessage(testInput);
        Message consumedMessage = testMessageConsumerQueue.receive(5000);
        Assert.assertEquals(((ObjectMessage) consumedMessage).getObject(), testInput);
    }

    /**
     * Test sending a long value and consuming the message.
     * @throws JMSException If an error occurs during JMS operations.
     */
    @Test
    public void testSendMessage_setupNoTransactionQueue_SendLong_thenConsumeMessage() throws JMSException{
        long testInput = Long.MAX_VALUE;
        testProducer.setup(true, queueName);
        testProducer.sendMessage(testInput);
        Message consumedMessage = testMessageConsumerQueue.receive(5000);
        Assert.assertEquals(((ObjectMessage) consumedMessage).getObject(),testInput);
    }

    /**
     * Test sending a char value and consuming the message.
     * @throws JMSException If an error occurs during JMS operations.
     */
    @Test
    public void testSendMessage_setupNoTransactionQueue_SendChar_thenConsumeMessage() throws JMSException{
        char testInput = 'c';
        testProducer.setup(true, queueName);
        testProducer.sendMessage(testInput);
        Message consumedMessage = testMessageConsumerQueue.receive(5000);
        Assert.assertEquals(((ObjectMessage) consumedMessage).getObject(),testInput);
    }

    /**
    * Test sending multiple messages with varying priority levels to a queue and then consumes them.
    * @throws JMSException if an error occurs in the Java Message Service.
    * @throws InterruptedException if the thread is interrupted while waiting.
    */
    @Test
    public void testSendPriorityMessage_setupNoTransactionQueue_SendInt_thenConsumeMessage() throws JMSException, InterruptedException {
        String[] messages = {"Msg One", "Msg two", "Msg three", "Msg four", "Msg five"};
        testProducer.setup(true, queueName);
        testProducer.sendMessage(messages[2],4);
        testProducer.sendMessage(messages[1], 7);
        testProducer.sendMessage(messages[0], 9);
        testProducer.sendMessage(messages[3], 3);
        testProducer.sendMessage(messages[4], 2);

        for (int i = 0; i < messages.length; i++) {
            Message message = testMessageConsumerQueue.receive(5000);
            Assert.assertEquals(((TextMessage) message).getText(), messages[i]);
        }
    }

    /**
     * Test sending an int value and consuming the message.
     * @throws JMSException If an error occurs during JMS operations.
     */
    @Test
    public void testSendMessage_setupNoTransactionQueue_SendInt_thenConsumeMessage() throws JMSException{
        int testInput = 22;
        testProducer.setup(true, queueName);
        testProducer.sendMessage(testInput);
        Message consumedMessage = testMessageConsumerQueue.receive(5000);
        Assert.assertEquals(((ObjectMessage) consumedMessage).getObject(),testInput);
    }

    /**
     * Test sending a double value and consuming the message.
     * @throws JMSException If an error occurs during JMS operations.
     */
    @Test
    public void testSendMessage_setupNoTransactionQueue_SendDouble_thenConsumeMessage() throws JMSException{
        double testInput = 22.2;
        testProducer.setup(true, queueName);
        testProducer.sendMessage(testInput);
        Message consumedMessage = testMessageConsumerQueue.receive(5000);
        var incMsg = ((ObjectMessage) consumedMessage).getObject();
        Assert.assertEquals(incMsg,testInput);
    }

    /**
     * Test sending a String value and consuming the message.
     * @throws JMSException If an error occurs during JMS operations.
     */
    @Test
    public void testSendMessage_setupNoTransactionQueue_SendText_thenConsumeMessage() throws JMSException{
        String testInput = "Test Text";
        testProducer.setup(true, queueName);
        testProducer.sendMessage(testInput);
        Message consumedMessage = testMessageConsumerQueue.receive(5000);
        Assert.assertEquals(((TextMessage) consumedMessage).getText(),testInput);
    }

    /**
     * Test sending a multiple text message with transaction and committing the session, then consuming the message.
     * @throws JMSException If an error occurs during JMS operations.
     */
    @Test
    public void testSendMessage_setupWithTransactionQueue_SendTextAndCommit_thenConsumeMessage() throws JMSException{
        String[] testInput = {"Test1", "Test2", "Test3"};
        testProducer.setTransacted(true);
        testProducer.setup(true, queueName);
        testProducer.sendMessage(testInput[0]);
        testProducer.sendMessage(testInput[1]);
        testProducer.sendMessage(testInput[2]);

        Message consumedMessage = testMessageConsumerQueue.receive(100);
        Assert.assertNull(consumedMessage);

        testProducer.commitMessages();
        for (int i = 0; i < testInput.length; i++) {
            Message message = testMessageConsumerQueue.receive(5000);
            Assert.assertEquals(((TextMessage) message).getText(),testInput[i]);
        }
    }


    /**
     * Test sending a text message to a topic, then consuming the message from multiple subscribers.
     * @throws JMSException If an error occurs during JMS operations.
     * @throws InterruptedException If thread interruption occurs during sleep.
     */
    @Test
    public void testSendMessage_setupNoTransactionTopic_subscribeOnTopic_thenSendText() throws JMSException, InterruptedException {
        Connection testConnection1 = new ActiveMQConnectionFactory().createConnection();
        Connection testConnection2 = new ActiveMQConnectionFactory().createConnection();
        testConnection1.start();
        testConnection2.start();
        Session testSession1 = testConnection1.createSession();
        Session testSession2 = testConnection2.createSession();
        Destination testDestination1 = testSession1.createTopic(topicName);
        Destination testDestination2 = testSession2.createTopic(topicName);
        MessageConsumer testConsumer1 = testSession1.createConsumer(testDestination1);
        MessageConsumer testConsumer2 = testSession2.createConsumer(testDestination2);
        ConsumerMessageListener consumer1 = new ConsumerMessageListener("TestConsumer1");
        ConsumerMessageListener consumer2 = new ConsumerMessageListener("TestConsumer2");
        testConsumer1.setMessageListener(consumer1);
        testConsumer2.setMessageListener(consumer2);

        Producer testedProducer = new Producer();
        testedProducer.setup(false, topicName);
        String testInput = "Test Text";
        testedProducer.sendMessage(testInput);

        Thread.sleep(100);
        testedProducer.close();

        Message consumedMessage1 = consumer1.getLastMessage();
        Message consumedMessage2 = consumer2.getLastMessage();

        Assert.assertEquals(((TextMessage)consumedMessage1).getText(),testInput);
        Assert.assertEquals(((TextMessage)consumedMessage2).getText(),testInput);

        close(testConsumer1, testSession1, testConnection1);
        close(testConsumer2, testSession2, testConnection2);
    }

    /**
     * Test sending a text message to a topic with transaction, then consuming the message from multiple subscribers.
     * @throws JMSException If an error occurs during JMS operations.
     * @throws InterruptedException If thread interruption occurs during sleep.
     */
    @Test
    public void testSendMessage_setupWithTransactionTopic_subscribeOnTopic_thenSendText() throws JMSException, InterruptedException {
        Connection testConnection1 = new ActiveMQConnectionFactory().createConnection();
        Connection testConnection2 = new ActiveMQConnectionFactory().createConnection();
        testConnection1.start();
        testConnection2.start();
        Session testSession1 = testConnection1.createSession();
        Session testSession2 = testConnection2.createSession();
        Destination testDestination1 = testSession1.createTopic(topicName);
        Destination testDestination2 = testSession2.createTopic(topicName);
        MessageConsumer testConsumer1 = testSession1.createConsumer(testDestination1);
        MessageConsumer testConsumer2 = testSession2.createConsumer(testDestination2);
        ConsumerMessageListener consumer1 = new ConsumerMessageListener("TestConsumer1");
        ConsumerMessageListener consumer2 = new ConsumerMessageListener("TestConsumer2");
        testConsumer1.setMessageListener(consumer1);
        testConsumer2.setMessageListener(consumer2);

        Producer testedProducer = new Producer();
        testedProducer.setTransacted(true);
        testedProducer.setup(false, topicName);
        String testInput = "Test Text";
        testedProducer.sendMessage(testInput);
        testedProducer.commitMessages();

        Thread.sleep(100);
        testedProducer.close();

        Message consumedMessage1 = consumer1.getLastMessage();
        Message consumedMessage2 = consumer2.getLastMessage();

        Assert.assertEquals(((TextMessage)consumedMessage1).getText(),testInput);
        Assert.assertEquals(((TextMessage)consumedMessage2).getText(),testInput);

        close(testConsumer1, testSession1, testConnection1);
        close(testConsumer2, testSession2, testConnection2);
    }

    /**
     * Test sending a byte array and consuming the message.
     * @throws JMSException If an error occurs during JMS operations.
     */
    @Test
    public void testSendMessage_setupNoTransactionQueue_SendByte_thenConsumeMessage() throws JMSException{
        byte[] testInput = {10,20,30,40,50};
        testProducer.setup(true, queueName);
        testProducer.sendMessage(testInput);
        Message consumedMessage = testMessageConsumerQueue.receive(5000);
        byte[] data = new byte[(int)((BytesMessage) consumedMessage).getBodyLength()];
        ((BytesMessage) consumedMessage).readBytes(data);
        Assert.assertArrayEquals(testInput, data);
    }

    /**
     * Test sending a byte array with transaction and committing the session, then consuming the message.
     * @throws JMSException If an error occurs during JMS operations.
     */
    @Test
    public void testSendMessage_setupWithTransactionQueue_SendByteCommit_thenConsumeMessage() throws JMSException{
        byte[] testInput = {10,20,30,40,50};
        testProducer.setTransacted(true);;
        testProducer.setup(true, queueName);
        testProducer.sendMessage(testInput);
        testProducer.commitMessages();
        Message consumedMessage = testMessageConsumerQueue.receive(5000);
        byte[] data = new byte[(int)((BytesMessage) consumedMessage).getBodyLength()];
        ((BytesMessage) consumedMessage).readBytes(data);
        Assert.assertArrayEquals(testInput, data);
    }

    /**
     * Test sending a map and consuming the message.
     * @throws JMSException If an error occurs during JMS operations.
     */
    @Test
    public void testSendMessage_setupNoTransactionQueue_SendMap_thenConsumeMessage() throws JMSException{
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
        testProducer.setup(true, queueName);
        testProducer.sendMessage(testInput);
        Message consumedMessage = testMessageConsumerQueue.receive(5000);
        Map<String, Object> map = new HashMap<>();
        Enumeration<String> keys = ((MapMessage) consumedMessage).getMapNames();
        while(keys.hasMoreElements()){
            String key = keys.nextElement();
            Object value = ((MapMessage) consumedMessage).getObject(key);
            map.put(key, value);
        }
        Assert.assertEquals(testInput, map);
    }

    /**
     * Test sending a map with transaction and committing the session, then consuming the message.
     * @throws JMSException If an error occurs during JMS operations.
     */
    @Test
    public void testSendMessage_setupWithTransactionQueue_SendMapCommit_thenConsumeMessage() throws JMSException{
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
        testProducer.setTransacted(true);
        testProducer.setup(true, queueName);
        testProducer.sendMessage(testInput);
        testProducer.commitMessages();
        Message consumedMessage = testMessageConsumerQueue.receive(5000);
        Map<String, Object> map = new HashMap<>();
        Enumeration<String> keys = ((MapMessage) consumedMessage).getMapNames();
        while(keys.hasMoreElements()){
            String key = keys.nextElement();
            Object value = ((MapMessage) consumedMessage).getObject(key);
            map.put(key, value);
        }
        Assert.assertEquals(testInput, map);
    }

    /**
     * Test sending a byte array as a map and consuming the message.
     * @throws JMSException If an error occurs during JMS operations.
     */
    @Test
    public void testSendMessage_setupNoTransactionQueue_SendByteArrayAsMap_thenConsumeMessage() throws JMSException{
         Map<String, Object> testInput = new HashMap<>();
        byte[] producedByte = (byte[]) "value009".getBytes();
        testInput.put("key_9", producedByte);
        testProducer.setup(true, queueName);
        testProducer.sendMessage(testInput);
        Message consumedMessage = testMessageConsumerQueue.receive(5000);
        byte[] consumedByte = (byte[]) ((MapMessage) consumedMessage)
                .getObject((String) ((MapMessage) consumedMessage).getMapNames().nextElement());
        Assert.assertArrayEquals(producedByte, consumedByte);
    }

    /**
     * Test sending a byte array as a map with transaction and committing the session, then consuming the message.
     * @throws JMSException If an error occurs during JMS operations.
     */
    @Test
    public void testSendMessage_setupWithTransactionQueue_SendByteArrayAsMapAndCommit_thenConsumeMessage() throws JMSException{
        Map<String, Object> testInput = new HashMap<>();
        byte[] producedByte = (byte[]) "value009".getBytes();
        testInput.put("key_9", producedByte);
        testProducer.setTransacted(true);
        testProducer.setup(true, queueName);
        testProducer.sendMessage(testInput);
        testProducer.commitMessages();
        Message consumedMessage = testMessageConsumerQueue.receive(5000);
        byte[] consumedByte = (byte[]) ((MapMessage) consumedMessage)
                .getObject((String) ((MapMessage) consumedMessage).getMapNames().nextElement());
        Assert.assertArrayEquals(producedByte, consumedByte);
    }

    /**
     * Test sending a stream of objects and consuming the message.
     * @throws JMSException If an error occurs during JMS operations.
     */
    @Test
    public void testSendMessage_setupNoTransactionQueue_SendStream_thenConsumeMessage() throws JMSException{
        Object[] testInput = {12, "StreamTest", '2'};
        testProducer.setup(true, queueName);
        testProducer.sendMessage(testInput);
        Message consumedMessage = testMessageConsumerQueue.receive(5000);
        StreamMessage streamMessage = (StreamMessage) consumedMessage;
        ArrayList<Object> payload = new ArrayList<>();
        while (true){
            try {
                payload.add(streamMessage.readObject());
            } catch (Exception e) {
                break;
            }
        }
        Assert.assertArrayEquals(testInput, payload.toArray());
    }

    /**
     * Test sending a stream of objects with transaction and committing the session, then consuming the message.
     * @throws JMSException If an error occurs during JMS operations.
     */
    @Test
    public void testSendMessage_setupWithTransactionQueue_SendStream_thenConsumeMessage() throws JMSException{
        Object[] testInput = {12, "StreamTest", '2'};
        testProducer.setTransacted(true);
        testProducer.setup(true, queueName);
        testProducer.sendMessage(testInput);
        testProducer.commitMessages();
        Message consumedMessage = testMessageConsumerQueue.receive(5000);
        StreamMessage streamMessage = (StreamMessage) consumedMessage;
        ArrayList<Object> payload = new ArrayList<>();
        while (true){
            try {
                payload.add(streamMessage.readObject());
            } catch (Exception e) {
                break;
            }
        }
        Assert.assertArrayEquals(testInput, payload.toArray());
    }

    /**
     * Test sending an object and consuming the message.
     * @throws JMSException If an error occurs during JMS operations.
     */
    @Test
    public void testSendMessage_setupNoTransactionQueue_SendObject_thenConsumeMessage() throws JMSException{
        int inputNumber = 1337;
        String inputName = "John Doe";
        TestObjectClass testObjectClass = new TestObjectClass(inputNumber, inputName);
        Producer producer = new Producer();
        producer.setup(true, queueName);
        producer.sendMessage(testObjectClass);
        Message consumedMessage = testMessageConsumerQueue.receive(5000);
        ObjectMessage aaa = ((ObjectMessage) consumedMessage);
        TestObjectClass testObjectClass1 = (TestObjectClass) aaa.getObject();
        Assert.assertEquals(inputName, testObjectClass1.getName());
        Assert.assertEquals(inputNumber, testObjectClass1.getNumber());
    }

    /**
     * Test sending an object with transaction and committing the session, then consuming the message.
     * @throws JMSException If an error occurs during JMS operations.
     */
    @Test
    public void testSendMessage_setupWithTransactionQueue_SendObject_thenConsumeMessage() throws JMSException{
        int inputNumber = 1337;
        String inputName = "John Doe";
        TestObjectClass testObjectClass = new TestObjectClass(inputNumber, inputName);
        Producer producer = new Producer();
        producer.setTransacted(true);
        producer.setup(true, queueName);
        producer.sendMessage(testObjectClass);
        producer.commitMessages();
        Message consumedMessage = testMessageConsumerQueue.receive(5000);
        ObjectMessage aaa = ((ObjectMessage) consumedMessage);
        TestObjectClass testObjectClass1 = (TestObjectClass) aaa.getObject();
        Assert.assertEquals(inputName, testObjectClass1.getName());
        Assert.assertEquals(inputNumber, testObjectClass1.getNumber());
    }

    /**
     * Closes the resources.
     * @param consumer  The message consumer to close.
     * @param session   The session to close.
     * @param connection    The connection to close.
     * @throws JMSException If an error occurs during JMS operations.
     */
    private void close(MessageConsumer consumer, Session session, Connection connection) throws JMSException {
        if (consumer != null){
            consumer.close();
            consumer = null;
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

    /**
     * Creates a test message consumer.
     * @param destination   The destination of the consumer.
     * @return  The created message consumer.
     * @throws JMSException If an error occurs during JMS operations.
     */
    private MessageConsumer createTestConsumer(String destination) throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
        ((ActiveMQConnectionFactory)connectionFactory).setTrustAllPackages(true);
        ((ActiveMQConnectionFactory)connectionFactory).setMessagePrioritySupported(true);
        Connection testConnection = connectionFactory.createConnection();
        testConnection.start();
        Session testSession = testConnection.createSession();
        Destination testDestination = testSession.createQueue(destination);
        return testSession.createConsumer(testDestination);
    }
}