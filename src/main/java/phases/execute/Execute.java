package phases.execute;

import JMS.Consumer;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.jms.JMSException;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.logging.Logger;
import java.util.Base64;

public class Execute implements Runnable{
    private static final Logger log = Logger.getLogger(Execute.class.getName());
    private static final String DEFAULT_SUBSCRIBED_CHANNEL = "plan-execute-queue";
    private static final String URL = "http://localhost:3880/api/v1/migrations/copy";

    private static final String dCACHE_SOURCE_POOL = "pool_write";
    private static final String dCACHE_TARGET_POOLS = "pool_res2";
    private Consumer consumer = null;
    private HttpClient client = null;
    private HttpRequest request = null;
    private enum receivedAdaptation{
        MIGRATION
    }


    /**
     * Constructs an Execute instance with specified JMS settings.
     *
     * @param queueBool indicates if the destination is a queue.
     * @param subscribedChannel the channel to subscribe to for receiving messages.
     * @throws JMSException if there is an error in setting up the JMS consumer.
     */
    public Execute(boolean queueBool, String subscribedChannel) throws JMSException {
        this.consumer = new Consumer();
        consumer.setup(queueBool, subscribedChannel);
        this.client = HttpClient.newHttpClient();
    }

    /**
     * Constructs an Execute instance with default JMS settings.
     *
     * @throws JMSException if there is an error in setting up the JMS consumer.
     */
    public Execute() throws JMSException {
        this(true, DEFAULT_SUBSCRIBED_CHANNEL);
    }

    /**
     * Continuously receives messages, selects the appropriate adaptation action,
     * and sends a REST-API request
     */
    @Override
    public void run(){
        while (true) {
            try {
                String messageReceived = (String)consumer.receive();
                do {
                    selectAdaptationAction(messageReceived);
                } while(adaptationAction());

            } catch (JMSException | IOException | InterruptedException e) {
                log.warning(this.getClass().getSimpleName() + ": " + e.getMessage());
                throw new RuntimeException(e);
            }
        }
    }


    /**
     * Selects the appropriate adaptation action based on the received message.
     * TODO: customize the JSON
     * @param messageReceived the message received from the plan-phase.
     */
    private void selectAdaptationAction(String messageReceived){
        if(receivedAdaptation.MIGRATION.name().equals(messageReceived)){
            JSONObject requestBody = new JSONObject();
            JSONArray targetPools = new JSONArray();
            targetPools.put(dCACHE_TARGET_POOLS);
            requestBody.put("sourcePool", dCACHE_SOURCE_POOL);
            requestBody.put("targetPools", targetPools);
            String creds = "admin#admin:dickerelch";

            request = HttpRequest.newBuilder()
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody.toString()))
                    .uri(URI.create(URL))
                    .header("Authorization", "Basic " + Base64.getEncoder().encodeToString(creds.getBytes()))
                    .header("Content-Type", "application/json")
                    .build();
        }
    }

    /**
     * Executes the adaptation action by sending the request.
     *
     * @return true if the action needs to be retried, otherwise false.
     * @throws IOException if there is an error in sending the HTTP request.
     * @throws InterruptedException if the HTTP request is interrupted.
     */
    private boolean adaptationAction() throws IOException, InterruptedException {
        log.info("Send request to: " + URL);
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 200 || response.statusCode() == 201 || response.statusCode() == 202) {
            log.info("Received statusCode: " + response.statusCode());
            return false;
        }
        log.info("Received statusCode: " + response.statusCode() + ", message: " + response.body());
        return true;
    }
}
