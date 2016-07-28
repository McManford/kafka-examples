package kafka.examples;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;


public class ConsumerAssign2 extends Thread
{
    private final KafkaConsumer<Integer, String> consumer;
    private final String topic;
    private final DateFormat df;
    private final String logTag;

    public ConsumerAssign2(Properties props)
    {
        this.df = new SimpleDateFormat("HH:mm:ss");
        logTag = "ConsumerAssign2";

        consumer = new KafkaConsumer<>(props);
        this.topic = props.getProperty("topic");
    }

    public void doWork() {
        //System.out.println(logTag + ": Doing work...");
        TopicPartition partition0 = new TopicPartition(this.topic, 1);
        consumer.assign(Arrays.asList(partition0));

        ConsumerRecords<Integer, String> records = consumer.poll(1000);
        Date now = Calendar.getInstance().getTime();

        for (ConsumerRecord<Integer, String> record : records) {
            int kafkaKey = record.key();
            String kafkaValue = record.value();
            System.out.println(this.df.format(now) + " " + logTag + ":" +
                    " Received: {" + kafkaKey + ":" + kafkaValue + "}" +
                    ", partition(" + record.partition() + ")" +
                    ", offset(" + record.offset() + ")");
        }

        List<ConsumerRecord<Integer, String>> partitionRecords = records.records(partition0);
        long batchSize = partitionRecords.size();
        System.out.println(this.df.format(now) + " " + logTag + ": batchSize: " + batchSize);
        if (batchSize > 0) {
            long firstOffset = partitionRecords.get(0).offset();
            System.out.println(this.df.format(now) + " " + logTag + ": firstOffset: " + firstOffset);
            //long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
            //consumer.commitSync(Collections.singletonMap(partition0, new OffsetAndMetadata(firstOffset + 2)));
        }

        consumer.commitSync();
    }

    public void run() {
        while (true) {
            doWork();
        }
    }
}
