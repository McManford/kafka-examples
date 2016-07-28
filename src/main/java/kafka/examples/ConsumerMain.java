package kafka.examples;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;


public class ConsumerMain
{
    public static void main(String[] args)
    {
        System.out.println("ConsumerMain");

        KafkaProperties kprops = new KafkaProperties();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kprops.KAFKA_BOOTSTRAP_SERVERS);
        props.put("topic", kprops.TOPIC);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kprops.GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kprops.AUTO_OFFSET_RESET);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        Consumer1 consumerThread = new Consumer1(props);
        consumerThread.start();
    }
}
