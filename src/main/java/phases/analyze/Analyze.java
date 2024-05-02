package phases.analyze;

import JMS.Producer;
import JMS.Consumer;
import org.json.JSONObject;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class Analyze {
    private static final Logger log = Logger.getLogger(Analyze.class.getName());
    private Consumer consumer = null;
    private Producer producer = null;
    private double value;

    public Analyze(boolean queueBool, String inputQueue, String outputQueue) throws JMSException {
        this.consumer = new Consumer();
        this.producer = new Producer();
        consumer.setup(queueBool, inputQueue);
        producer.setup(queueBool, outputQueue);
    }

    public void run() throws JMSException {
        while (true) {
            Message abc = consumer.receiveMessage();
            String ttt = ((TextMessage) abc).getText();
            JSONObject monData = new JSONObject(ttt);
            doThings(monData);
            log.info(this.getClass().getSimpleName() + " send: " + value);
            producer.sendMessage(value);
        }
    }

    private void doThings(JSONObject monitoringData){ //TODO: utilityFunc()

        if (monitoringData == null || monitoringData.isEmpty()) {
            log.info("No monitoring data received");
            return;
        }
        log.info("Received monitoring data: " + monitoringData.toString());

         /*
        List<Double> resultList = new ArrayList<>();

       String[] elements = text.substring(1, text.length() - 1).split(", ");

        for (String element : elements) {
            try {
                if (!element.isEmpty()) {
                    double value = Double.parseDouble(element);
                    resultList.add(value);
                }
            } catch (NumberFormatException e) {
                System.err.println("Invalid double value: " + element);
            }
        }

        double tmp = 0;
        for (double i : resultList) {
            tmp += i;
        }
        this.value = tmp /resultList.size();*/
    }

}
