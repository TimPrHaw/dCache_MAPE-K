package phases.monitor;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Logger;
import java.util.stream.Stream;


public class BillingRecords implements MessageReceiver {
    private static final Logger log = Logger.getLogger(BillingRecords.class.getName());
    /**
     * Find out the operating system type first (supported Linux , not supported: Windows, MacOS)
     */
    private final static String OS_TYPE = System.getProperty("os.name").toLowerCase();
    private Integer billingLineCounter;
    private Integer smartLineCounter;

    private boolean mockingSmartCtl = false;

    /**
     * Path to the billing file TODO:!!!!! PLEASE CHANGE THIS PATH TO YOUR LOCAL PATH !!!!!
     */
    private final String billingPath = "./dcache-build-75393-billing.json";
    private final String smartPath = "./smart_one_line.json";

    private final int LINES_IN_BILLING_RECORD = 2000;
    private final int LINES_IN_SMART_RECORD = 1;//20; //TODO: change to whatever is needed


    public BillingRecords() {
        log.info("OS_TYPE is: " + OS_TYPE);

        if (!isSmartCtlInstalled() || !OS_TYPE.contains("nux")) {
            System.out.println("smartctl is not available, proceeding with mocks...!");
            mockingSmartCtl = true;
        }
        this.billingLineCounter = 0;
        this.smartLineCounter = 0;
    }

    @Override
    public JSONObject receiveMessage() {
        JSONObject diskInfo = null;
        JSONObject billingInfo = null;
        JSONObject combinedInfo = null;
        try {
            diskInfo = this.getDiskInfo();
            billingInfo = this.getBillingInfo();
            combinedInfo = this.combineTwoJsonObjects(diskInfo, billingInfo);
            return combinedInfo;
        } catch (IOException  | NullPointerException  | UnsupportedOperationException  | JSONException e) {
            throw new RuntimeException(e);
        }
    }

    private JSONObject getBillingInfo() {
        Object transferSize = null;
        JSONObject billingJSON = null;
        String noBilling = "{\"transferSize\":null}";

        billingJSON = readJsonLineByLine(billingPath, billingLineCounter);
        billingLineCounter++;
        if (billingLineCounter >= LINES_IN_BILLING_RECORD) {
            billingLineCounter = 0;
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

    /**
     * This method returns a json object containing status information about the system's disk
     * @return JSONObject (can be empty, if keys or values are null) or null
     * @throws IOException
     * @throws NullPointerException
     * @throws UnsupportedOperationException
     *
     */
    public JSONObject getDiskInfo() throws IOException, NullPointerException, UnsupportedOperationException, JSONException {

        JSONObject jsonObject = new JSONObject();
        String jsonString = "";
        String device = "Mock-up";
        JSONObject analyseJSON = new JSONObject();
        if (mockingSmartCtl) {
            System.out.println("getDiskInfo: smartctl is not available, proceeding with mock..!");
            jsonObject = readJsonLineByLine(smartPath, smartLineCounter);
            smartLineCounter++;
            if (smartLineCounter >= LINES_IN_SMART_RECORD) {
                smartLineCounter = 0;
            }
        }
        else {
            try {
                ProcessBuilder pb1 = new ProcessBuilder("lsblk", "-n", "--output", "NAME", "--nodeps");
                Process proc1 = pb1.start();
                device = new String(proc1.getInputStream().readAllBytes()).trim();
                ProcessBuilder pb2 = new ProcessBuilder("sudo", "smartctl", "-j", "-a", "/dev/" + device);
                Process proc2 = pb2.start();
                jsonString = new String(proc2.getInputStream().readAllBytes());
                if (jsonString == null || jsonString.isEmpty()) {
                    System.out.println("No disk information found!");
                    return analyseJSON;
                }
                jsonObject = new JSONObject(jsonString);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
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
            log.info("Combined JSON object: " + jsonObject.toString());
        } catch (JSONException e) {
            throw new JSONException("Error combining two JSON objects: " + e.getMessage());
        }
        return jsonObject;
    }
    private boolean isSmartCtlInstalled() {
        ProcessBuilder pb = new ProcessBuilder("smartctl", "-V");
        try {
            Process proc = pb.start();
            String version = new String(proc.getInputStream().readAllBytes());
            if (version == null || version.isEmpty()) {
                return false;
            }
            return true;
        } catch (IOException e) {
            log.info("smartctl: Something went wrong: " + e.getMessage());
        }
        return false;
    }
    private JSONObject readJsonLineByLine(String path, Integer lineCounter) {
        String line;
        JSONObject billingJSON = null;
        try (Stream<String> lines = Files.lines(Paths.get(path))) {
            line = lines.skip(lineCounter).findFirst().get();
            billingJSON = new JSONObject(line);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return billingJSON;
    }
}
