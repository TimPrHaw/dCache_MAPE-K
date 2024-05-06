package phases.monitor;

import JMS.Producer;
import org.json.JSONException;
import org.json.JSONObject;

import javax.jms.JMSException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Logger;
import java.util.stream.Stream;


public class MonitorBilling implements Runnable {
    private static final Logger log = Logger.getLogger(MonitorBilling.class.getName());
    /**
     * Find out the operating system type first (Linux, MacOS, not supported: Windows)
     */
    private final static String OS_TYPE = System.getProperty("os.name").toLowerCase();
    private  MessageReceiver messageReceiver;
    private Producer producer;
    private Integer lineCounter;
    private final Integer THREAD_SLEEP_TIME = 5000;

    /**
     * Path to the billing file TODO:!!!!! PLEASE CHANGE THIS PATH TO YOUR LOCAL PATH !!!!!
     */
    private final String path = "./dcache-build-75393-billing.json";

    private final int LINES_IN_BILLING_RECORD = 2000;


    public MonitorBilling(String messageReceiver, String topic, String bootstrapServers) throws JMSException {
        log.info("OS_TYPE is: %24s%n" + OS_TYPE);

        if (!OS_TYPE.contains("nux") && !OS_TYPE.contains("darwin")) {
            System.out.println("OS not supported!");
            System.exit(1);
        }
        producer = new Producer();
        producer.setup("analyze-queue");
        this.lineCounter = 0;
        if (topic != null || bootstrapServers != null) {
            this.messageReceiver = MessageReceiverFactory.createMessageReceiver(messageReceiver, topic, bootstrapServers);
            log.info(this.getClass().getName() + " built messageReceiver: " + this.messageReceiver.toString());
        }
    }
    public MonitorBilling(String messageReceiver) throws JMSException {
        this(messageReceiver, null, null);
    }

    @Override
    public void run(){
        JSONObject diskInfo = null;
        JSONObject billingInfo = null;
        JSONObject combinedInfo = null;
        while(true){
            try {
                diskInfo = this.getDiskInfo();
                billingInfo = this.getBillingInfo();
                combinedInfo = this.combineTwoJsonObjects(diskInfo, billingInfo);
                pushToJMS(combinedInfo);
                Thread.sleep(THREAD_SLEEP_TIME);
            } catch (IOException  | NullPointerException  | UnsupportedOperationException  | JSONException
                     | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
    private JSONObject getBillingInfo() {
        Object transferSize = null;
        JSONObject billingJSON = null;
        String line = null;
        String noBilling = "{\"transferSize\":null}";

        try (Stream<String> lines = Files.lines(Paths.get(path))) {
            line = lines.skip(lineCounter).findFirst().get();
//            System.out.println(line);
            billingJSON = new JSONObject(line);
        } catch (IOException e) {
            e.printStackTrace();
        }
        lineCounter++;
        if (lineCounter == LINES_IN_BILLING_RECORD) {
            lineCounter = 0;
        }
        if (billingJSON == null || billingJSON.isEmpty()){
            log.info("No billing data found!");
            return new JSONObject(noBilling);
        }
        if (billingJSON.opt("msgType").equals("request")){
            transferSize = billingJSON.optJSONObject("moverInfo", new JSONObject()).opt("transferSize");
        }
        if (transferSize != null) {
            return new JSONObject().putOpt("transferSize", transferSize);
        }
        return new JSONObject(noBilling);
    }
    private void pushToJMS(JSONObject monitoringData){
        try {
            producer.sendMessage(monitoringData.toString());
        } catch (JMSException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * This method returns a json object containing status information about the system's disk
     * @return JSONObject (can be empty, if keys or values are null) or null
     * @throws IOException
     * @throws NullPointerException
     * @throws UnsupportedOperationException
     *
     */
    public JSONObject getDiskInfo() throws IOException, NullPointerException, UnsupportedOperationException, JSONException {

        JSONObject jsonObject = null;
        JSONObject analyseJSON = new JSONObject();

        if (OS_TYPE.contains("nux")) {

            ProcessBuilder pb1 = new ProcessBuilder("lsblk", "-n", "--output", "NAME", "--nodeps");
            Process proc1 = pb1.start();
            String device = new String(proc1.getInputStream().readAllBytes()).trim();
            ProcessBuilder pb2 = new ProcessBuilder("sudo", "smartctl", "-j", "-a", "/dev/" + device);
            Process proc2 = pb2.start();
            String jsonString = new String(proc2.getInputStream().readAllBytes());
            if (jsonString == null || jsonString.isEmpty()) {
                System.out.println("No disk information found!");
                return null;
            }
            jsonObject = new JSONObject(jsonString);
            Object model_name = jsonObject.opt("model_name");
            Object num_err_log_entries = jsonObject.optJSONObject("nvme_smart_health_information_log", new JSONObject()).opt("num_err_log_entries");
            Object media_errors = jsonObject.optJSONObject("nvme_smart_health_information_log", new JSONObject()).opt("media_errors");
            Object critical_warning = jsonObject.optJSONObject("nvme_smart_health_information_log", new JSONObject()).opt("critical_warning");
            System.out.println();
            System.out.printf("Disk device name: %20s%n", device);
            System.out.printf("Model name: %49s%n", model_name);
            System.out.printf("Number of Error-log entries: %5d%n", num_err_log_entries);
            System.out.printf("Media Errors: %18d%n", media_errors);
            System.out.printf("Critical Warning: %14d%n", critical_warning);

            analyseJSON.putOpt("model_name", model_name);
            analyseJSON.putOpt("num_err_log_entries", num_err_log_entries);
            analyseJSON.putOpt("media_errors", media_errors);
            analyseJSON.putOpt("critical_warning", critical_warning);
            System.out.println(analyseJSON);
        }
        else if (OS_TYPE.contains("darwin")) {
            //todo: implement for MacOS
//            ProcessBuilder pb1 = new ProcessBuilder("lsblk", "-n", "--output", "NAME", "--nodeps");
//            Process proc1 = pb1.start();
//            String device = new String(proc1.getInputStream().readAllBytes());
//            System.out.println(device.trim());
//            ProcessBuilder pb2 = new ProcessBuilder("sudo", "smartctl", "-j", "-a", "/dev/" + device.trim());
//            Process proc2 = pb2.start();
//            String jsonString = new String(proc2.getInputStream().readAllBytes());
//            System.out.println(jsonString);
//            JSONObject jsonObject = new JSONObject(jsonString);
//            System.out.println(jsonObject);
//            System.out.println(jsonObject.get("serial_number"));
//            System.out.println(jsonObject.getJSONObject("temperature").get("current"));
        }
        return analyseJSON;
    }
    /**
     * This method returns a json object combining two input json objects
     * @return JSONObject or null
     * @throws JSONException
     */
    private JSONObject combineTwoJsonObjects(JSONObject jsonObject1, JSONObject jsonObject2) throws JSONException{
        JSONObject jsonObject = null;
        try {
            jsonObject = new JSONObject(jsonObject1, JSONObject.getNames(jsonObject1));
            for (String key : JSONObject.getNames(jsonObject2)) {
                jsonObject.put(key, jsonObject2.get(key));
            }
        } catch (JSONException e) {
            throw new JSONException("Error combining two JSON objects: " + e.getMessage());
        }
        return jsonObject;
    }
}
