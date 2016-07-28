package kafka.examples;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public class Consumer2 extends Thread
{
    private final KafkaConsumer<Integer, String> consumer;
    private final String topic;
    private final DateFormat df;
    private final String logTag;

    public Consumer2(Properties props)
    {
        logTag = "Consumer2";

        consumer = new KafkaConsumer<>(props);
        this.topic = props.getProperty("topic");
        this.df = new SimpleDateFormat("HH:mm:ss");

        consumer.subscribe(Collections.singletonList(this.topic));
    }

    public void doWork() {
        //System.out.println(logTag + ": Doing work...");

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
    }

    public void run() {
        while (true) {
            doWork();
        }
    }
}