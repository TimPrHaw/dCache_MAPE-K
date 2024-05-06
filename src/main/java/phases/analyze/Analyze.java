package phases.analyze;

import JMS.Producer;
import JMS.Consumer;

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
        consumer.setup(inputQueue);
        producer.setup(outputQueue);
    }

    public void run() throws JMSException {
        while (true){
            String inputString = consumer.receive();
            String decidedOutput = decisionFunction(inputString);
            log.info(this.getClass().getSimpleName() + " send: " + decidedOutput);
            producer.sendMessage(decidedOutput);
        }
    }

    private double utilityFunction(String inputString){
        double w1 = 0.7;
        double w2 = 0.375;
        double v1 = 2;
        double v2 = 2;
        return w1 * v1 + w2 * v2;
    }

    private String decisionFunction(String inputValue){
        String outputString = "";
        double u = utilityFunction(inputValue);
        if(u <= 0){
            outputString = "case1";
        } else if (u >= 0 && u <= 0.5) {
            outputString = "case2";
        } else if (u >= 0.5 && u <= 1) {
            outputString = "case3";
        } else {
            outputString = "case4";
        }
        return outputString;
    }


    private void doThings(String text){
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
        this.value = tmp /resultList.size();
    }

}
