package kafka.examples;

public class KafkaProperties {
    public static final String TOPIC = "topic1";
    public static final String TOPIC2 = "topic2";
    public static final String TOPIC3 = "topic3";
    public static final String KAFKA_BOOTSTRAP_SERVERS = "10.218.86.43:9092 10.218.86.237:9092";
    public static final String KAFKA_SERVER_URL = "localhost";
    public static final int KAFKA_SERVER_PORT = 9092;
    public static final int KAFKA_PRODUCER_BUFFER_SIZE = 64 * 1024;
    public static final int CONNECTION_TIMEOUT = 100000;
    final static int RECONNECT_INTERVAL = 10000;
    public static final String CLIENT_ID = "SimpleConsumerDemoClient";
    final static  String GROUP_ID = "group1";
    final static String AUTO_OFFSET_RESET = "earliest";
    //final static String AUTO_OFFSET_RESET = "latest";

    public KafkaProperties() {}
}
