package be.bagofwords.main;

import java.net.UnknownHostException;

public class JobServerMapping {

    public static String getJobServerAddress() {
        String hostname;
        try {
            hostname = java.net.InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException("Failed to find localhost name!", e);
        }
        switch (hostname) {
            case "koen-desktop":
                return "localhost";
            case "calvin":
                return "144.76.251.61";
            case "hobbes":
                return "localhost";
            default:
                throw new RuntimeException("Unknown host " + hostname);
        }
    }

}
