package phases.execute;

import JMS.Consumer;
import org.json.JSONObject;

import javax.jms.JMSException;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.logging.Logger;

public class Execute implements Runnable{
    private static final Logger log = Logger.getLogger(Execute.class.getName());
    private final String URL = "http://localhost:3000/posts";
    private Consumer consumer = null;
    private HttpClient client = null;
    private HttpRequest request = null;
    private enum receivedAdaptation{
        MIGRATION
    }

    public Execute(boolean queueBool, String subscribedChannel) throws JMSException {
        this.consumer = new Consumer();
        consumer.setup(queueBool, subscribedChannel);
        this.client = HttpClient.newHttpClient();
    }

    public Execute() throws JMSException {
        this(true, "plan-execute-queue");
    }

    @Override
    public void run(){
        while (true) {
            try {
                String executionAction = null; //TODO: delete?
                String messageReceived = (String)consumer.receive();
                do {
                    selectAdaptationAction(messageReceived);
                } while(adaptationAction());

            } catch (JMSException | IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private String selectAdaptationAction(String messageReceived){
        if(receivedAdaptation.MIGRATION.name().equals(messageReceived)){
            JSONObject requstBody = new JSONObject();
            JSONObject data = new JSONObject();
            requstBody.put("name", "Migration");
            data.put("Attribute 1", 123);
            data.put("Attribute 2", "POST Request in Java");
            requstBody.put("data", data);

            request = HttpRequest.newBuilder()
                    .POST(HttpRequest.BodyPublishers.ofString(requstBody.toString()))
                    .uri(URI.create(URL))
                    .header("Content-Type", "application/json")
                    .build();
        }
        return null;
    }

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
