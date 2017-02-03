package kafka.examples;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;


public class ConsumerRebalanceListener2 extends Thread
{
    private final KafkaConsumer<Integer, String> consumer;
    private final String topic;
    private final DateFormat df;
    private final String logTag = "ConsumerRebalanceListener2";

    public ConsumerRebalanceListener2(Properties props)
    {
        consumer = new KafkaConsumer<>(props);
        this.topic = props.getProperty("topic");
        this.df = new SimpleDateFormat("HH:mm:ss");

        consumer.subscribe(Collections.singletonList(this.topic), new MyConsumerRebalanceListener2(consumer));

        //TopicPartition partition0 = new TopicPartition(this.topic, 0);
        //consumer.assign(Arrays.asList(partition0));
    }

    public void doWork() {
        //System.out.println(logTag + ": Doing work...");

        ConsumerRecords<Integer, String> records = consumer.poll(1000);
        Date now = Calendar.getInstance().getTime();

        int recordsCount = records.count();
        System.out.println(this.df.format(now) + " " + logTag + ":" +
                " poll returned " + recordsCount + " records");

        for (ConsumerRecord<Integer, String> record : records) {
            int kafkaKey = record.key();
            String kafkaValue = record.value();
            System.out.println(this.df.format(now) + " " + logTag + ":" +
                    " Received: {" + kafkaKey + ":" + kafkaValue + "}" +
                    ", topic: " + record.topic() +
                    ", partition: " + record.partition() +
                    ", offset: " + record.offset());

//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }

        consumer.commitSync();
    }

    public void run() {
        while (true) {
            doWork();
        }
    }
}