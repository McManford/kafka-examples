package kafka.examples;


import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public class MyConsumerRebalanceListener1 implements org.apache.kafka.clients.consumer.ConsumerRebalanceListener {

    private final String logTag = "MyConsumerRebalanceListener1";

    //private OffsetManager offsetManager = new OffsetManager("storage2");
    private Consumer<String, String> consumer;

    public MyConsumerRebalanceListener1(Consumer consumer) {
        this.consumer = consumer;
    }

    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

        for (TopicPartition partition : partitions) {
            System.out.println(logTag + ": was revoked partition: " + partition.toString());
            // offsetManager.saveOffsetInExternalStore(partition.topic(), partition.partition(), consumer.position(partition));
        }
    }

    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

        for (TopicPartition partition : partitions) {
            System.out.println(logTag + ": was assigned partition: " + partition.toString());
//            consumer.seek(partition, offsetManager.readOffsetFromExternalStore(partition.topic(), partition.partition()));
        }
    }
}