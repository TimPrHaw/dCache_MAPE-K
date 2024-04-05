package JMS.producer;

import javax.jms.JMSException;

import JMS.Producer;
import org.apache.activemq.*;

import javax.jms.*;
import javax.jms.Message;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class ProducerTest {

    private String queueName = "test-queue";

    @Test
    public void setupNoTransactionQueue_SendText_thenConsumeMessage() throws JMSException{
        Producer testedProducer = new Producer();
        testedProducer.setup(false, true, queueName);
        String testInput = "Test Text";
        testedProducer.sendMessage(testInput);

        Connection testConnection = new ActiveMQConnectionFactory().createConnection();
        testConnection.start();
        Session testSession = testConnection.createSession();
        Destination testDestination = testSession.createQueue(queueName);
        MessageConsumer testConsumer = testSession.createConsumer(testDestination);
        Message consumedMessage = testConsumer.receive();

        Assert.assertEquals(((TextMessage) consumedMessage).getText(),testInput);

        Assert.assertFalse(consumedMessage instanceof BytesMessage);
        Assert.assertFalse(consumedMessage instanceof MapMessage);

        // Close producer and consumer
        testedProducer.close();
        close(testConsumer, testSession, testConnection);
    }


    @Test
    public void setupWithTransactionQueue_SendTextAndCommit_thenConsumeMessage() throws JMSException{
        Producer testedProducer = new Producer();
        testedProducer.setup(true, true, queueName);
        String testInput = "Test Text";
        testedProducer.sendMessage(testInput);
        testedProducer.commitSession(true);

        // Minimal Test Consumer
        Connection testConnection = new ActiveMQConnectionFactory().createConnection();
        testConnection.start();
        Session testSession = testConnection.createSession();
        Destination testDestination = testSession.createQueue(queueName);
        MessageConsumer testConsumer = testSession.createConsumer(testDestination);
        Message consumedMessage = testConsumer.receive();

        Assert.assertEquals(((TextMessage) consumedMessage).getText(),testInput);

        Assert.assertFalse(consumedMessage instanceof BytesMessage);
        Assert.assertFalse(consumedMessage instanceof MapMessage);

        // Close producer and consumer
        testedProducer.close();
        close(testConsumer, testSession, testConnection);
    }

    // TODO: TOPIC TEST FEHLERHAFT
    @Test
    public void setupNoTransactionTopic_SendText_thenConsumeMessage() throws JMSException{
        Producer testedProducer = new Producer();
        testedProducer.setup(false, false, queueName);
        String testInput = "Test Text";
        testedProducer.sendMessage(testInput);

        Connection testConnection = new ActiveMQConnectionFactory().createConnection();
        testConnection.start();
        Session testSession = testConnection.createSession();
        Destination testDestination = testSession.createTopic(queueName);
        MessageConsumer testConsumer = testSession.createConsumer(testDestination);
        Message consumedMessage = testConsumer.receive();

        Assert.assertEquals(((TextMessage) consumedMessage).getText(),testInput);

        Assert.assertFalse(consumedMessage instanceof BytesMessage);
        Assert.assertFalse(consumedMessage instanceof MapMessage);

        // Close producer and consumer
        testedProducer.close();
        close(testConsumer, testSession, testConnection);
    }


    @Test
    public void setupWithTransactionTopic_SendTextAndCommit_thenConsumeMessage() throws JMSException{
        Producer testedProducer = new Producer();
        testedProducer.setup(true, false, queueName);
        String testInput = "Test Text";
        testedProducer.sendMessage(testInput);
        testedProducer.commitSession(true);

        // Minimal Test Consumer
        Connection testConnection = new ActiveMQConnectionFactory().createConnection();
        testConnection.start();
        Session testSession = testConnection.createSession();
        Destination testDestination = testSession.createTopic(queueName);
        MessageConsumer testConsumer = testSession.createConsumer(testDestination);
        Message consumedMessage = testConsumer.receive();

        Assert.assertEquals(((TextMessage) consumedMessage).getText(),testInput);

        Assert.assertFalse(consumedMessage instanceof BytesMessage);
        Assert.assertFalse(consumedMessage instanceof MapMessage);

        // Close producer and consumer
        testedProducer.close();
        close(testConsumer, testSession, testConnection);
    }

    @Test
    public void setupNoTransactionQueue_SendByte_thenConsumeMessage() throws JMSException{
        Producer testedProducer = new Producer();
        testedProducer.setup(false, true, queueName);
        byte[] testInput = {10,20,30,40,50};
        testedProducer.sendMessage(testInput);

        // Minimal Test Consumer
        Connection testConnection = new ActiveMQConnectionFactory().createConnection();
        testConnection.start();
        Session testSession = testConnection.createSession();
        Destination testDestination = testSession.createQueue(queueName);
        MessageConsumer testConsumer = testSession.createConsumer(testDestination);
        Message consumedMessage = testConsumer.receive();

        byte[] data = new byte[(int)((BytesMessage) consumedMessage).getBodyLength()];
        ((BytesMessage) consumedMessage).readBytes(data);

        Assert.assertArrayEquals(testInput, data);

        Assert.assertFalse(consumedMessage instanceof TextMessage);
        Assert.assertFalse(consumedMessage instanceof MapMessage);

        // Close producer and consumer
        testedProducer.close();
        close(testConsumer, testSession, testConnection);
    }

    @Test
    public void setupWithTransactionQueue_SendByteCommit_thenConsumeMessage() throws JMSException{
        Producer testedProducer = new Producer();
        testedProducer.setup(true, true, queueName);
        byte[] testInput = {10,20,30,40,50};
        testedProducer.sendMessage(testInput);
        testedProducer.commitSession(true);

        // Minimal Test Consumer
        Connection testConnection = new ActiveMQConnectionFactory().createConnection();
        testConnection.start();
        Session testSession = testConnection.createSession();
        Destination testDestination = testSession.createQueue(queueName);
        MessageConsumer testConsumer = testSession.createConsumer(testDestination);
        Message consumedMessage = testConsumer.receive();

        byte[] data = new byte[(int)((BytesMessage) consumedMessage).getBodyLength()];
        ((BytesMessage) consumedMessage).readBytes(data);

        Assert.assertArrayEquals(testInput, data);

        Assert.assertFalse(consumedMessage instanceof TextMessage);
        Assert.assertFalse(consumedMessage instanceof MapMessage);

        // Close producer and consumer
        testedProducer.close();
        close(testConsumer, testSession, testConnection);
    }

    @Test
    public void setupNoTransactionQueue_SendMap_thenConsumeMessage() throws JMSException{
        Producer testedProducer = new Producer();
        testedProducer.setup(false, true, queueName);

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
        testedProducer.sendMessage(testInput);

        // Minimal Test Consumer
        Connection testConnection = new ActiveMQConnectionFactory().createConnection();
        testConnection.start();
        Session testSession = testConnection.createSession();
        Destination testDestination = testSession.createQueue(queueName);
        MessageConsumer testConsumer = testSession.createConsumer(testDestination);
        Message consumedMessage = testConsumer.receive();

        Map<String, Object> map = new HashMap<>();
        Enumeration<String> keys = ((MapMessage) consumedMessage).getMapNames();
        while(keys.hasMoreElements()){
            String key = keys.nextElement();
            Object value = ((MapMessage) consumedMessage).getObject(key);
            map.put(key, value);
        }

        Assert.assertEquals(testInput, map);

        Assert.assertFalse(consumedMessage instanceof BytesMessage);
        Assert.assertFalse(consumedMessage instanceof TextMessage);

        // Close producer and consumer
        testedProducer.close();
        close(testConsumer, testSession, testConnection);
    }

    @Test
    public void setupNoTransactionQueue_SendByteArrayAsMap_thenConsumeMessage() throws JMSException{
        Producer testedProducer = new Producer();
        testedProducer.setup(false, true, queueName);

        Map<String, Object> testInput = new HashMap<>();
        byte[] producedByte = (byte[]) "value009".getBytes();
        testInput.put("key_9", producedByte);

        testedProducer.sendMessage(testInput);

        // Minimal Test Consumer
        Connection testConnection = new ActiveMQConnectionFactory().createConnection();
        testConnection.start();
        Session testSession = testConnection.createSession();
        Destination testDestination = testSession.createQueue(queueName);
        MessageConsumer testConsumer = testSession.createConsumer(testDestination);
        Message consumedMessage = testConsumer.receive();

        byte[] consumedByte = (byte[]) ((MapMessage) consumedMessage)
                .getObject((String) ((MapMessage) consumedMessage).getMapNames().nextElement());

        Assert.assertArrayEquals(producedByte, consumedByte);

        Assert.assertFalse(consumedMessage instanceof BytesMessage);
        Assert.assertFalse(consumedMessage instanceof TextMessage);

        // Close producer and consumer
        testedProducer.close();
        close(testConsumer, testSession, testConnection);
    }

    @Test
    public void setupWithTransactionQueue_SendMapCommit_thenConsumeMessage() throws JMSException{
        Producer testedProducer = new Producer();
        testedProducer.setup(true, true, queueName);

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

        testedProducer.sendMessage(testInput);
        testedProducer.commitSession(true);

        // Minimal Test Consumer
        Connection testConnection = new ActiveMQConnectionFactory().createConnection();
        testConnection.start();
        Session testSession = testConnection.createSession();
        Destination testDestination = testSession.createQueue(queueName);
        MessageConsumer testConsumer = testSession.createConsumer(testDestination);
        Message consumedMessage = testConsumer.receive();

        Map<String, Object> map = new HashMap<>();
        Enumeration<String> keys = ((MapMessage) consumedMessage).getMapNames();
        while(keys.hasMoreElements()){
            String key = keys.nextElement();
            Object value = ((MapMessage) consumedMessage).getObject(key);
            map.put(key, value);
        }

        Assert.assertEquals(testInput, map);
        Assert.assertTrue(consumedMessage instanceof MapMessage);

        Assert.assertFalse(consumedMessage instanceof BytesMessage);
        Assert.assertFalse(consumedMessage instanceof TextMessage);

        // Close producer and consumer
        testedProducer.close();
        close(testConsumer, testSession, testConnection);
    }

    @Test
    public void setupWithTransactionQueue_SendByteArrayAsMapAndCommit_thenConsumeMessage() throws JMSException{
        Producer testedProducer = new Producer();
        testedProducer.setup(true, true, queueName);
        Map<String, Object> testInput = new HashMap<>();
        byte[] producedByte = (byte[]) "value009".getBytes();
        testInput.put("key_9", producedByte);

        testedProducer.sendMessage(testInput);
        testedProducer.commitSession(true);

        // Minimal Test Consumer
        Connection testConnection = new ActiveMQConnectionFactory().createConnection();
        testConnection.start();
        Session testSession = testConnection.createSession();
        Destination testDestination = testSession.createQueue(queueName);
        MessageConsumer testConsumer = testSession.createConsumer(testDestination);
        Message consumedMessage = testConsumer.receive();

        byte[] consumedByte = (byte[]) ((MapMessage) consumedMessage)
                .getObject((String) ((MapMessage) consumedMessage).getMapNames().nextElement());

        Assert.assertArrayEquals(producedByte, consumedByte);
        Assert.assertTrue(consumedMessage instanceof MapMessage);

        Assert.assertFalse(consumedMessage instanceof BytesMessage);
        Assert.assertFalse(consumedMessage instanceof TextMessage);

        // Close producer and consumer
        testedProducer.close();
        close(testConsumer, testSession, testConnection);
    }

    /**
     * TODO: Streams und Object testen
    @Test
    public void setupNoTransactionQueue_SendSteam_thenConsumeMessage() throws JMSException{
        Producer testedProducer = new Producer();
        testedProducer.setup(false, true, queueName);

        Object[] testInput = {12, "StreamTest", '2'};

        testedProducer.sendMessage(testInput);

        // Minimal Test Consumer
        Connection testConnection = new ActiveMQConnectionFactory().createConnection();
        testConnection.start();
        Session testSession = testConnection.createSession();
        Destination testDestination = testSession.createQueue(queueName);
        MessageConsumer testConsumer = testSession.createConsumer(testDestination);
        Message consumedMessage = testConsumer.receive();

        //((StreamMessage) consumedMessage).

        //Assert.assertArrayEquals(testInput, data);

        Assert.assertFalse(consumedMessage instanceof TextMessage);
        Assert.assertFalse(consumedMessage instanceof MapMessage);

        // Close producer and consumer
        testedProducer.close();
        close(testConsumer, testSession, testConnection);
    }



    @Test
    public void setupNoTransactionQueue_SendObject_thenConsumeMessage() throws JMSException{

        System.setProperty("org.apache.activemq.SERIALIZABLE_PACKAGES", "package JMS.producer");

        Producer testedProducer = new Producer();
        testedProducer.setup(false, true, queueName);
        int inputNumber = 1337;
        String inputName = "John Doe";
        TestObjectClass testObjectClass = new TestObjectClass(inputNumber, inputName);

        testedProducer.sendMessage(testObjectClass);

        // Minimal Test Consumer
        Connection testConnection = new ActiveMQConnectionFactory().createConnection();
        testConnection.start();
        Session testSession = testConnection.createSession();
        Destination testDestination = testSession.createQueue(queueName);
        MessageConsumer testConsumer = testSession.createConsumer(testDestination);
        Message consumedMessage = testConsumer.receive();

        ObjectMessage objectMessage = (ObjectMessage) consumedMessage;
        TestObjectClass receivedObject = (TestObjectClass) objectMessage.getObject();

        //Assert.assertArrayEquals(testInput, data);

        Assert.assertFalse(consumedMessage instanceof TextMessage);
        Assert.assertFalse(consumedMessage instanceof MapMessage);

        // Close producer and consumer
        testedProducer.close();
        close(testConsumer, testSession, testConnection);
    }
    */

    /**
    private Message consume(boolean isQueue, String destination) throws JMSException {
    MessageConsumer consumer = testConsumer(isQueue, destination);
    return consumer.receive();
    }


    private MessageConsumer testConsumer(boolean isQueue, String destination) throws JMSException {
        Connection testConnection = new ActiveMQConnectionFactory().createConnection();
        testConnection.start();
        Session testSession = testConnection.createSession();
        Destination testDestination;
        if (isQueue) {
            testDestination = testSession.createQueue(destination);
        } else {
            testDestination = testSession.createTopic(destination);
        }
        return testSession.createConsumer(testDestination);
    }
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
}

