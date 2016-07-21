package kafka.examples;

import java.util.Map;


public class DatabusMessage {
    final Map<String, String> headers;
    final byte [] payload;

    public DatabusMessage(Map<String, String> headers, byte [] payload) {
        this.headers = headers;
        this.payload = payload;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public byte[] getPayload() {
        return payload;
    }
}
