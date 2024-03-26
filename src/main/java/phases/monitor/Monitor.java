package phases.monitor;

import kafka.KafkaListener;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

public class Monitor {
    private static final Logger log = Logger.getLogger(KafkaListener.class.getName());
    //private final KafkaListener consumer;
    private final List<Double> list;
    private final int LISTSIZE = 10;

    public Monitor(){
        list = new LinkedList<>();
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

    }
    // TODO: JMS EINRICHTEN UND kommunikation mit Analyze herstellen

    public List<Double> getList() {
        return list;
    }
}
