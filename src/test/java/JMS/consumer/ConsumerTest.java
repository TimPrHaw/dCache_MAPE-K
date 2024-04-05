package JMS.consumer;

import JMS.Producer;
import JMS.SynchConsumer;
import JMS.testClass.TestObjectClass;
import org.junit.Assert;
import org.junit.Test;

import javax.jms.*;
import java.util.HashMap;
import java.util.Map;

public class ConsumerTest {
    String queueName = "test-consumer-queue";

    @Test
    public void setupNoTransactionQueue_ConsumeTextMessage_runGetMessage() throws JMSException {

        String testMessage = "Hello World";

        SynchConsumer synchConsumer = new SynchConsumer();
        synchConsumer.setup(true, queueName);

        Producer producer = new Producer();
        producer.setup(true, queueName);
        producer.sendMessage(testMessage);

        Message test = synchConsumer.runGetMessage();
        TextMessage textMessage = (TextMessage) test;

        Assert.assertNotNull(test);
        Assert.assertEquals(testMessage, textMessage.getText());

        synchConsumer.close();
        producer.close();
    }

    @Test
    public void setupNoTransactionQueue_ConsumeByteMessage() throws JMSException {
        byte[] testInput = {10,20,30,40,50};

        SynchConsumer synchConsumer = new SynchConsumer();
        synchConsumer.setup(true, queueName);

        Producer producer = new Producer();
        producer.setup(true, queueName);
        producer.sendMessage(testInput);

        byte[] test = synchConsumer.run();

        Assert.assertNotNull(test);
        Assert.assertArrayEquals(testInput, test);

        synchConsumer.close();
        producer.close();
    }

    @Test
    public void setupNoTransactionQueue_ConsumeMapMessage() throws JMSException {
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

        SynchConsumer synchConsumer = new SynchConsumer();
        synchConsumer.setup(true, queueName);

        Producer producer = new Producer();
        producer.setup(true, queueName);
        producer.sendMessage(testInput);

        Map<String, Object> test = synchConsumer.run();

        Assert.assertNotNull(test);
        Assert.assertEquals(testInput, test);

        synchConsumer.close();
        producer.close();
    }

    @Test
    public void setupNoTransactionQueue_ConsumeTextMessage() throws JMSException {
        String testMessage = "Hello World";

        SynchConsumer synchConsumer = new SynchConsumer();
        synchConsumer.setup(true, queueName);

        Producer producer = new Producer();
        producer.setup(true, queueName);
        producer.sendMessage(testMessage);

        String test = synchConsumer.run();

        Assert.assertNotNull(test);
        Assert.assertEquals(testMessage, test);

        synchConsumer.close();
        producer.close();
    }

    @Test
    public void setupNoTransactionQueue_ConsumeObjectMessage() throws JMSException {
        //String queueName = "test-consumer-queue1";
        TestObjectClass testObjectClass = new TestObjectClass(1, "TestName1");

        SynchConsumer synchConsumer = new SynchConsumer();
        synchConsumer.setup(true, queueName);

        Producer producer = new Producer();
        producer.setup(true, queueName);
        producer.sendMessage(testObjectClass);

        ObjectMessage test = synchConsumer.run();

        TestObjectClass testObjectClassConsume = (TestObjectClass) test.getObject();

        Assert.assertNotNull(test);
        Assert.assertEquals(testObjectClass.toString(), testObjectClassConsume.toString());

        synchConsumer.close();
        producer.close();
    }

    @Test
    public void setupNoTransactionQueue_ConsumeStreamMessage() throws JMSException {
        //String queueName = "test-consumer-queue2";
        Object[] testInput = {12, "StreamTest", '2'};

        SynchConsumer synchConsumer = new SynchConsumer();
        synchConsumer.setup(true, queueName);

        Producer producer = new Producer();
        producer.setup(true, queueName);
        producer.sendMessage(testInput);

        Object[] test = synchConsumer.run();


        Assert.assertNotNull(test);
        Assert.assertArrayEquals(testInput, test);

        synchConsumer.close();
        producer.close();
    }
}
