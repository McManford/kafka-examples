package kafka.examples;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;


public class Consumer2 extends Thread
{
    private final AtomicBoolean closed = new AtomicBoolean(false);
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

        Date now = Calendar.getInstance().getTime();
        System.out.println(this.df.format(now) + " " + logTag + ": Doing work...");
        // consumer.partitionsFor(this.topic);
        System.out.println(this.df.format(now) + " " + logTag + ": polling...");

        ConsumerRecords<Integer, String> records = consumer.poll(1000);

        for (ConsumerRecord<Integer, String> record : records) {
            int kafkaKey = record.key();
            String kafkaValue = record.value();
            System.out.println(this.df.format(now) + " " + logTag + ":" +
                    " Received: {" + kafkaKey + ":" + kafkaValue + "}" +
                    ", topic: " + record.topic() +
                    ", partition: " + record.partition() +
                    ", offset: " + record.offset());
        }
    }

    public void run() {

        try {
            while (!closed.get()) {
                doWork();
            }
        } catch (WakeupException e) {
            // if (!closed.get()) throw e;
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {

        closed.set(true);
        consumer.wakeup();
    }

    public void wakeUp() {
        consumer.wakeup();
    }
}