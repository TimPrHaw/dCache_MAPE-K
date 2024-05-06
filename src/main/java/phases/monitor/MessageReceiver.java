package phases.monitor;

import org.json.JSONObject;

/**
 * Interface representing a message receiver.
 */
public interface MessageReceiver {
    /**
     * Receives a message.
     * @return The received message as a JSONObject.
     */
    JSONObject receiveMessage();
}
