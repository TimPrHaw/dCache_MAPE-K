package phases.plan;

import JMS.SynchConsumer;

import javax.jms.JMSException;

public class Plan {
    private SynchConsumer consumer = null;

    public Plan() throws JMSException {
        this.consumer = new SynchConsumer();
        consumer.setup(true, "AnalyzeTopic");
    }

    public void run() throws JMSException {
        while (true) {
            consumer.run();
        }
    }
}
