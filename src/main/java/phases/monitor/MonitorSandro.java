package phases.monitor;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

public class MonitorSandro {

    private final static String OS_TYPE = System.getProperty("os.name").toLowerCase();
    public MonitorSandro() throws IOException {
        System.out.printf("OS_TYPE is: %24s%n!", OS_TYPE);

        if (!OS_TYPE.contains("nux") && !OS_TYPE.contains("darwin")) {
            System.out.println("OS not supported!");
            System.exit(1);
        }
    }

    public static void main(String[] args) throws IOException {

        MonitorSandro monitor = new MonitorSandro();
        JSONObject smartctl_output_json = monitor.getDiskInfo();
        System.out.println(smartctl_output_json);
    }

    /**
     * This method returns a json object containing the status of the system's disk
     * @return JSONObject or null
     * @throws IOException
     * @throws NullPointerException
     * @throws UnsupportedOperationException
     *
     */
    public JSONObject getDiskInfo() throws IOException, NullPointerException, UnsupportedOperationException, JSONException {

        JSONObject jsonObject = null;

        if (OS_TYPE.contains("nux")) {

            ProcessBuilder pb1 = new ProcessBuilder("lsblk", "-n", "--output", "NAME", "--nodeps");
            Process proc1 = pb1.start();
            String device = new String(proc1.getInputStream().readAllBytes()).trim();
            ProcessBuilder pb2 = new ProcessBuilder("sudo", "smartctl", "-j", "-a", "/dev/" + device);
            Process proc2 = pb2.start();
            String jsonString = new String(proc2.getInputStream().readAllBytes());
//            System.out.println(jsonString);
            jsonObject = new JSONObject(jsonString);
//            System.out.println(jsonObject);
            String model_name = jsonObject.get("model_name").toString();
            Object num_err_log_entries = jsonObject.getJSONObject("nvme_smart_health_information_log").get("num_err_log_entries");
            Object media_errors = jsonObject.getJSONObject("nvme_smart_health_information_log").get("media_errors");
            Object critical_warning = jsonObject.getJSONObject("nvme_smart_health_information_log").get("critical_warning");
            System.out.printf("Disk device name: %20s%n", device);
            System.out.printf("Model name: %49s%n", model_name);
            System.out.printf("Number of Error-log entries: %5d%n", num_err_log_entries);
            System.out.printf("Media Errors: %18d%n", media_errors);
            System.out.printf("Critical Warning: %14d%n", critical_warning);
//            System.out.println(jsonObject.getJSONObject("temperature").get("current"));
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
        return jsonObject;
    }
    public JSONObject parseSmartctlOutput(JSONObject jsonObject) {
        return new JSONObject(jsonObject);
    }














}
