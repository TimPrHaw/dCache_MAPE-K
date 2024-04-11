package phases.monitor;

import JMS.Producer;

import javax.jms.JMSException;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

public class Monitor implements Runnable {
    private static final Logger log = Logger.getLogger(Monitor.class.getName());
    private final MessageReceiver messageReceiver;
    private final List<Double> list;
    private final int LISTSIZE = 4;
    //private KafkaCons kafkaCons;
    private Producer producer;

    public Monitor(String messageReceiver, String topic, String bootstrapServers) throws JMSException {
        list = new LinkedList<>();
        producer = new Producer();
        producer.setup("analyze-queue");
        this.messageReceiver = MessageReceiverFactory.createMessageReceiver(messageReceiver, topic, bootstrapServers);
        log.info(this.getClass().getName() + " built messageReceiver: " + this.messageReceiver.toString());
    }

    public Monitor(String messageReceiver) throws JMSException {
        this(messageReceiver, null, null);
    }

    @Override
    public void run(){
        while(true){
            List<String> valueList = messageReceiver.messageReceive();
            addList(valueList);
            pushToJMS();
        }
    }

    public void addList(List<String> elementList){
        try {
            for (String element : elementList) {
                if(list.size() == LISTSIZE){
                    list.remove(0);
                }
                list.add(Double.parseDouble(element));
                log.info("List update: " + list);
            }
        } catch (NumberFormatException e){
            log.info("Ungültiges Format für die Umwandlung in einen double-Wert: " + elementList);
            e.printStackTrace();
        }

    }

    private void pushToJMS(){
        try {
            producer.sendMessage(getList());
        } catch (JMSException ex) {
            ex.printStackTrace();
        }
    }

    private String getList() {
        String t = list.toString();
        return t;
    }
}
