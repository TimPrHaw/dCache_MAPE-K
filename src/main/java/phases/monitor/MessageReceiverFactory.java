package phases.monitor;

import kafka.KafkaCons;

/**
 * A factory class for creating message receivers.
 */
public abstract class MessageReceiverFactory {
    /**
     * Creates a message receiver based on the provided parameters.
     * @param receiverType The type of message receiver.
     * @param topic The topic name.
     * @param bootstrapServers The bootstrap servers.
     * @return The created message receiver.
     */
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
