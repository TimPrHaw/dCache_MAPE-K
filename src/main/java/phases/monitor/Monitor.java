package phases.monitor;

import JMS.Producer;
import kafka.KafkaListener;

import javax.jms.JMSException;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

public class Monitor {
    private static final Logger log = Logger.getLogger(Monitor.class.getName());
    //private final KafkaListener consumer;
    private final List<Double> list;
    private final int LISTSIZE = 4;
    private Producer producer;

    public Monitor(){
        list = new LinkedList<>();
        try {
            setProducer();
        }
        catch (JMSException ex) {}
    }

    public void setProducer() throws JMSException {
        producer = new Producer();
        producer.setup(true, "analyze-queue");
    }

    public void addList(String element){
        if(list.size() == LISTSIZE){
            list.remove(0);
        }
        try {
            list.add(Double.parseDouble(element));
            log.info("List update: " + list);
        } catch (NumberFormatException e){
            log.info("Ungültiges Format für die Umwandlung in einen double-Wert: " + element);
            e.printStackTrace();
        }
        pushToJMS();
    }

    private void pushToJMS(){
        try {
            producer.sendMessage(getList());
        } catch (JMSException ex) {}
    }

    public String getList() {
        String t = list.toString();
        return t;
    }
}
