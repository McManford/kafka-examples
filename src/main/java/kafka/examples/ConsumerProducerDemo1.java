package kafka.examples;

import java.io.File;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;


public class ConsumerProducerDemo1
{
    public static void main(String[] args)
    {
        System.out.println("ConsumerProducerDemo1");
        for (int i = 0; i < args.length; i++)
            System.out.println("args[" + i + "]:" + args[i]);
        
        boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
        int messagesToProduce = args.length == 0 ? -1 : Integer.parseInt(args[1].trim());

        KafkaProperties kprops = new KafkaProperties();

        Properties prodProps = new Properties();
        prodProps.put("bootstrap.servers", "127.0.0.1:9093");
        prodProps.put("topic", "topic1");
        prodProps.put("client.id", "DemoProducer");
        prodProps.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        prodProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prodProps.put("max.block.ms", 10000);
        prodProps.put("request.timeout.ms", 20000);
        // Authentication using SASL PLAIN. This username and password should be set in KafkaServer section of Kafka
        // JAAS config file.
        prodProps.put("security.protocol", "SASL_PLAINTEXT");
        prodProps.put("sasl.mechanism", "PLAIN");
        prodProps.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required \n" +
                "        username=\"alice\" \n" +
                "        password=\"alice-secret\";");
        //
        //prodProps.put("security.protocol", "SSL");
        //prodProps.put("ssl.truststore.location", getCertsDir()+"kafka.client.truststore.jks");
        //prodProps.put("ssl.truststore.password", "test1234");
        //prodProps.put("ssl.keystore.location", getCertsDir()+"kafka.client.keystore.jks");
        //prodProps.put("ssl.keystore.password", "test1234");
        //prodProps.put("ssl.key.password", "test1234");

        Properties consProps = new Properties();
        consProps.put("bootstrap.servers", "localhost:9093");
        consProps.put("topic", "topic1");
        consProps.put("group.id", kprops.GROUP_ID);
        consProps.put("auto.offset.reset", kprops.AUTO_OFFSET_RESET);
        consProps.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, "2000");
        consProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000");
        consProps.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, "2000");
        consProps.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, "2000");
        consProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "1000");
        consProps.put("session.timeout.ms", "1500");
        consProps.put("enable.auto.commit", "true");
        consProps.put("auto.commit.interval.ms", "1000");
        consProps.put("heartbeat.interval.ms", "1000");

        consProps.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        consProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //Consumer1 consumerThread = new Consumer1(consProps);
        //consumerThread.start();

        try {
            Thread.sleep(0);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Producer1 producerThread = new Producer1(prodProps, isAsync, messagesToProduce);
        producerThread.start();
    }

    protected static String getCertsDir() {
        File jarPath = new File(ProducerMain.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        return jarPath.getParentFile() + File.separator + ".." + File.separator + ".." + File.separator +
                "certs" + File.separator;
    }
}
