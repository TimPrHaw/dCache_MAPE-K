package phases.monitor;

import java.util.List;

/**
 * Interface representing a message receiver.
 */
public interface MessageReceiver {
    /**
     * Receives a message.
     * @return The received message as a list of strings.
     */
    List<String> messageReceive();
}
