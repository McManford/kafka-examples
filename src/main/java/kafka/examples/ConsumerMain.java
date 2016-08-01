package kafka.examples;

import java.io.File;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;


public class ConsumerMain
{
    public static void main(String[] args)
    {
        System.out.println("ConsumerMain");

        Properties props = new Properties();
        //props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-2:9093");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put("topic", "topic1");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        //props.put("security.protocol", "SSL");
        //props.put("ssl.truststore.location", getCertsDir()+"kafka.client.truststore.jks");
        //props.put("ssl.truststore.password", "test1234");
        //props.put("ssl.keystore.location", getCertsDir()+"kafka.client.keystore.jks");
        //props.put("ssl.keystore.password", "test1234");
        //props.put("ssl.key.password", "test1234");

        Consumer1 consumerThread = new Consumer1(props);
        consumerThread.start();
    }

    protected static String getCertsDir() {
        File jarPath = new File(ProducerMain.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        return jarPath.getParentFile() + File.separator + ".." + File.separator + ".." + File.separator +
                "certs" + File.separator;
    }
}
