package phases.monitor;

import kafka.KafkaCons;

public abstract class MessageReceiverFactory {
    public static MessageReceiver createMessageReceiver(String receiverType, String topic, String bootstrapServers) {
        if (topic != null && !topic.isEmpty()) {
            if (receiverType.equalsIgnoreCase("KAFKA")) {
                return new KafkaCons(topic, bootstrapServers);
            } else if (receiverType.equalsIgnoreCase("JSON")) {
                System.out.println("New JSON MessageReceiver");
                // TODO: ADD Klasse die JSON werte liefert
                // return JSON werte
                return null;
            }
        }
        System.out.println("New JSON MessageReceiver");
        // return JSON werte
        return null;
    }
}
