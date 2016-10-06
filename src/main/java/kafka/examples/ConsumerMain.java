package kafka.examples;

import java.io.File;
import java.util.Properties;


public class ConsumerMain
{
    public static void main(String[] args)
    {
        System.out.println("ConsumerMain");

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("topic", "topic1");
        props.put("group.id", "group1");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("heartbeat.interval.ms", "3000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

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
