package kafka.examples;

import java.io.File;
import java.util.Properties;


public class ProducerMain
{
    public static void main(String[] args)
    {
        boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
        int messagesToProduce = args.length == 0 ? -1 : Integer.parseInt(args[1].trim());

        System.out.println("ProducerMain");

        KafkaProperties kprops = new KafkaProperties();

        Properties props = new Properties();
        props.put("topic", kprops.TOPIC);
        props.put("bootstrap.servers", kprops.KAFKA_BOOTSTRAP_SERVERS);
        props.put("client.id", "DemoProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("max.block.ms", 10000);
        props.put("request.timeout.ms", 20000);

        Producer1 producerThread = new Producer1(props, isAsync, messagesToProduce);
        producerThread.start();
    }

    protected static  String getConfigFileName() {
        File jarPath = new File(ProducerMain.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        return jarPath.getParentFile() + File.separator + ".." + File.separator + ".." + File.separator +
                "config" + File.separator + "producer.properties";
    }
}
