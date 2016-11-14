package kafka.examples;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public class Consumer1 extends Thread
{
    private final KafkaConsumer<Integer, String> consumer;
    private final String topic;
    private final DateFormat df;
    private final String logTag;
    private boolean moreData = true;
    private boolean gotData = false;
    private int messagesReceived = 0;
    AtomicBoolean isRunning = new AtomicBoolean(true);
    CountDownLatch shutdownLatch = new CountDownLatch(1);

    public Consumer1(Properties props)
    {
        logTag = "Consumer1";

        consumer = new KafkaConsumer<>(props);
        this.topic = props.getProperty("topic");
        this.df = new SimpleDateFormat("HH:mm:ss");

        consumer.subscribe(Collections.singletonList(this.topic));
    }

    public void getMessages() {
        System.out.println("Getting messages...");
        while (moreData == true) {
            //System.out.println(logTag + ": polling...");
            ConsumerRecords<Integer, String> records = consumer.poll(1000);
            Date now = Calendar.getInstance().getTime();
            int recordsCount = records.count();
            messagesReceived += recordsCount;
            //System.out.println("poll returned " + recordsCount + " records");
            if (recordsCount > 0) {
               gotData = true;
            }

            if (gotData && recordsCount == 0) {
                moreData = false;
            }

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
        System.out.println("Received " + messagesReceived + " messages");
    }

    public void run() {
        getMessages();

        //while (isRunning.get()) {

        //}
        //shutdownLatch.countDown();
    }

    public void shutdown() {
        try {
            isRunning.set(false);
            this.interrupt();
            shutdownLatch.await();
        } catch (InterruptedException e) {
            throw new Error("Interrupted when shutting down consumer worker thread.");
        }
    }
}