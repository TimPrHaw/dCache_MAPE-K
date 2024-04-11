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

    private String queueName = "test-queue";
    private String topicName = "test/topic";
    private Producer testProducer;
    private MessageConsumer testMessageConsumerQueue;

    @Before
    public void setUp() throws Exception {
        testProducer = new Producer();
        testMessageConsumerQueue = createTestConsumer(queueName);
         }

    @After
    public void tearDown() throws Exception {
        testProducer.close();
        testMessageConsumerQueue.close();
    }

    @Test
    public void setupNoTransactionQueue_SendShort_thenConsumeMessage() throws JMSException{
        short testInput = Short.MIN_VALUE;
        testProducer.setup(false, true, queueName);
        testProducer.sendMessage(testInput);

        Message consumedMessage = testMessageConsumerQueue.receive(5000);

        Assert.assertEquals(((ObjectMessage) consumedMessage).getObject(), testInput);
    }

    @Test
    public void setupNoTransactionQueue_SendLong_thenConsumeMessage() throws JMSException{
        long testInput = Long.MAX_VALUE;

        testProducer.setup(false, true, queueName);
        testProducer.sendMessage(testInput);

        Message consumedMessage = testMessageConsumerQueue.receive(5000);

        Assert.assertEquals(((ObjectMessage) consumedMessage).getObject(),testInput);
    }

    @Test
    public void setupNoTransactionQueue_SendChar_thenConsumeMessage() throws JMSException{
        char testInput = 'c';

        testProducer.setup(false, true, queueName);
        testProducer.sendMessage(testInput);

        Message consumedMessage = testMessageConsumerQueue.receive(5000);

        Assert.assertEquals(((ObjectMessage) consumedMessage).getObject(),testInput);
    }

    @Test
    public void setupNoTransactionQueue_SendInt_thenConsumeMessage() throws JMSException{
        int testInput = 22;

        testProducer.setup(false, true, queueName);
        testProducer.sendMessage(testInput);

        Message consumedMessage = testMessageConsumerQueue.receive(5000);

        Assert.assertEquals(((ObjectMessage) consumedMessage).getObject(),testInput);
    }

    @Test
    public void setupNoTransactionQueue_SendDouble_thenConsumeMessage() throws JMSException{
        double testInput = 22.2;

        testProducer.setup(false, true, queueName);
        testProducer.sendMessage(testInput);

        Message consumedMessage = testMessageConsumerQueue.receive(5000);

        var incMsg = ((ObjectMessage) consumedMessage).getObject();

        Assert.assertEquals(incMsg,testInput);
    }

    @Test
    public void setupNoTransactionQueue_SendText_thenConsumeMessage() throws JMSException{
        String testInput = "Test Text";

        testProducer.setup(false, true, queueName);
        testProducer.sendMessage(testInput);

        Message consumedMessage = testMessageConsumerQueue.receive(5000);

        Assert.assertEquals(((TextMessage) consumedMessage).getText(),testInput);
    }

    @Test
    public void setupWithTransactionQueue_SendTextAndCommit_thenConsumeMessage() throws JMSException{
        String testInput = "Test Text";

        testProducer.setup(true, true, queueName);
        testProducer.sendMessage(testInput);
        testProducer.commitSession(true);

        Message consumedMessage = testMessageConsumerQueue.receive(5000);

        Assert.assertEquals(((TextMessage) consumedMessage).getText(),testInput);
    }

    @Test
    public void setupNoTransactionTopic_subscribeOnTopic_thenSendText() throws JMSException, InterruptedException {
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
        testedProducer.setup(false, false, topicName);
        String testInput = "Test Text";
        testedProducer.sendMessage(testInput);

        // kurzer sleep damit die Nachricht auch ankommt
        Thread.sleep(100);
        testedProducer.close();

        Message consumedMessage1 = consumer1.getLastMessage();
        Message consumedMessage2 = consumer2.getLastMessage();

        Assert.assertEquals(((TextMessage)consumedMessage1).getText(),testInput);
        Assert.assertEquals(((TextMessage)consumedMessage2).getText(),testInput);

        close(testConsumer1, testSession1, testConnection1);
        close(testConsumer2, testSession2, testConnection2);
    }

    @Test
    public void setupWithTransactionTopic_subscribeOnTopic_thenSendText() throws JMSException, InterruptedException {
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
        testedProducer.setup(true, false, topicName);
        String testInput = "Test Text";
        testedProducer.sendMessage(testInput);
        testedProducer.commitSession(true);

        // kurzer sleep damit die Nachricht auch ankommt
        Thread.sleep(100);
        testedProducer.close();

        Message consumedMessage1 = consumer1.getLastMessage();
        Message consumedMessage2 = consumer2.getLastMessage();

        Assert.assertEquals(((TextMessage)consumedMessage1).getText(),testInput);
        Assert.assertEquals(((TextMessage)consumedMessage2).getText(),testInput);

        close(testConsumer1, testSession1, testConnection1);
        close(testConsumer2, testSession2, testConnection2);
    }

    @Test
    public void setupNoTransactionQueue_SendByte_thenConsumeMessage() throws JMSException{
        byte[] testInput = {10,20,30,40,50};

        testProducer.setup(false, true, queueName);
        testProducer.sendMessage(testInput);

        Message consumedMessage = testMessageConsumerQueue.receive(5000);

        byte[] data = new byte[(int)((BytesMessage) consumedMessage).getBodyLength()];
        ((BytesMessage) consumedMessage).readBytes(data);

        Assert.assertArrayEquals(testInput, data);
    }

    @Test
    public void setupWithTransactionQueue_SendByteCommit_thenConsumeMessage() throws JMSException{
        byte[] testInput = {10,20,30,40,50};

        testProducer.setup(true, true, queueName);
        testProducer.sendMessage(testInput);
        testProducer.commitSession(true);

        Message consumedMessage = testMessageConsumerQueue.receive(5000);

        byte[] data = new byte[(int)((BytesMessage) consumedMessage).getBodyLength()];
        ((BytesMessage) consumedMessage).readBytes(data);

        Assert.assertArrayEquals(testInput, data);
    }

    @Test
    public void setupNoTransactionQueue_SendMap_thenConsumeMessage() throws JMSException{
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

        testProducer.setup(false, true, queueName);
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

    @Test
    public void setupWithTransactionQueue_SendMapCommit_thenConsumeMessage() throws JMSException{
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


        testProducer.setup(true, true, queueName);
        testProducer.sendMessage(testInput);
        testProducer.commitSession(true);

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

    @Test
    public void setupNoTransactionQueue_SendByteArrayAsMap_thenConsumeMessage() throws JMSException{
         Map<String, Object> testInput = new HashMap<>();
        byte[] producedByte = (byte[]) "value009".getBytes();
        testInput.put("key_9", producedByte);

        testProducer.setup(false, true, queueName);
        testProducer.sendMessage(testInput);

        Message consumedMessage = testMessageConsumerQueue.receive(5000);

        byte[] consumedByte = (byte[]) ((MapMessage) consumedMessage)
                .getObject((String) ((MapMessage) consumedMessage).getMapNames().nextElement());

        Assert.assertArrayEquals(producedByte, consumedByte);
    }

    @Test
    public void setupWithTransactionQueue_SendByteArrayAsMapAndCommit_thenConsumeMessage() throws JMSException{
        Map<String, Object> testInput = new HashMap<>();
        byte[] producedByte = (byte[]) "value009".getBytes();
        testInput.put("key_9", producedByte);

        testProducer.setup(true, true, queueName);
        testProducer.sendMessage(testInput);
        testProducer.commitSession(true);

        Message consumedMessage = testMessageConsumerQueue.receive(5000);

        byte[] consumedByte = (byte[]) ((MapMessage) consumedMessage)
                .getObject((String) ((MapMessage) consumedMessage).getMapNames().nextElement());

        Assert.assertArrayEquals(producedByte, consumedByte);
    }

    @Test
    public void setupNoTransactionQueue_SendStream_thenConsumeMessage() throws JMSException{
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

    @Test
    public void setupWithTransactionQueue_SendStream_thenConsumeMessage() throws JMSException{
        Object[] testInput = {12, "StreamTest", '2'};

        testProducer.setup(true, true, queueName);
        testProducer.sendMessage(testInput);
        testProducer.commitSession(true);

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

    @Test
    public void setupNoTransactionQueue_SendObject_thenConsumeMessage() throws JMSException{
        int inputNumber = 1337;
        String inputName = "John Doe";
        TestObjectClass testObjectClass = new TestObjectClass(inputNumber, inputName);

        Producer producer = new Producer();
        producer.setup(false, true, queueName);
        producer.sendMessage(testObjectClass);

        Message consumedMessage = testMessageConsumerQueue.receive(5000);

        ObjectMessage aaa = ((ObjectMessage) consumedMessage);
        TestObjectClass testObjectClass1 = (TestObjectClass) aaa.getObject();

        Assert.assertEquals(inputName, testObjectClass1.getName());
        Assert.assertEquals(inputNumber, testObjectClass1.getNumber());
    }

    @Test
    public void setupWithTransactionQueue_SendObject_thenConsumeMessage() throws JMSException{
        int inputNumber = 1337;
        String inputName = "John Doe";
        TestObjectClass testObjectClass = new TestObjectClass(inputNumber, inputName);

        Producer producer = new Producer();
        producer.setup(true, true, queueName);
        producer.sendMessage(testObjectClass);
        producer.commitSession(true);

        Message consumedMessage = testMessageConsumerQueue.receive(5000);

        ObjectMessage aaa = ((ObjectMessage) consumedMessage);
        TestObjectClass testObjectClass1 = (TestObjectClass) aaa.getObject();

        Assert.assertEquals(inputName, testObjectClass1.getName());
        Assert.assertEquals(inputNumber, testObjectClass1.getNumber());
    }

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

    private MessageConsumer createTestConsumer(String destination) throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
        ((ActiveMQConnectionFactory)connectionFactory).setTrustAllPackages(true);
        Connection testConnection = connectionFactory.createConnection();
        testConnection.start();
        Session testSession = testConnection.createSession();
        Destination testDestination = testSession.createQueue(destination);
        return testSession.createConsumer(testDestination);
    }
}

