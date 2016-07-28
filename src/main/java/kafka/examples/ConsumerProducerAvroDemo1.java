package kafka.examples;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;


public class ConsumerProducerAvroDemo1 {
    public static void main(String[] args)
    {
        boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
        int messagesToProduce = args.length == 0 ? -1 : Integer.parseInt(args[1].trim());

        System.out.println("ConsumerProducerAvroDemo1");

        KafkaProperties kprops = new KafkaProperties();

        Properties prodProps = new Properties();
        prodProps.put("bootstrap.servers", kprops.KAFKA_BOOTSTRAP_SERVERS);
        prodProps.put("topic", kprops.TOPIC);
        prodProps.put("client.id", "DemoProducer");

        Properties consProps = new Properties();
        consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kprops.KAFKA_BOOTSTRAP_SERVERS);
        consProps.put("topic", kprops.TOPIC);
        consProps.put(ConsumerConfig.GROUP_ID_CONFIG, kprops.GROUP_ID);
        consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kprops.AUTO_OFFSET_RESET);
        consProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        consProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
        consProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");

        ConsumerAvro consumerThread = new ConsumerAvro(consProps);
        consumerThread.start();

        try {
            Thread.sleep(0);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ProducerAvro producerThread = new ProducerAvro(prodProps, isAsync, messagesToProduce);
        producerThread.start();
    }
}
