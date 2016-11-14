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
import org.apache.kafka.common.serialization.IntegerDeserializer;

import kafka.examples.serializers.AvroDeserializer;


public class ConsumerAvro extends Thread
{
    AtomicBoolean isRunning = new AtomicBoolean(true);
    CountDownLatch shutdownLatch = new CountDownLatch(1);

    private final KafkaConsumer<Integer, DatabusMessage> consumer;
    private final String topic;
    private final DateFormat df;
    private final String logTag;

    public ConsumerAvro(Properties props)
    {
        logTag = "ConsumerAvro";

        final IntegerDeserializer keyDeserializer = new IntegerDeserializer();
        //keyDeserializer.configure(avroProps, true);
        final AvroDeserializer avroValueDeserializer = new AvroDeserializer();
        //avroValuedeserializer.configure(avroProps, false);

        consumer = new KafkaConsumer<>(props, keyDeserializer, avroValueDeserializer);
        this.topic = props.getProperty("topic");
        this.df = new SimpleDateFormat("HH:mm:ss");

        consumer.subscribe(Collections.singletonList(this.topic));
    }

    public void run() {
        while (isRunning.get()) {
            //System.out.println(logTag + ": Doing work...");

            ConsumerRecords<Integer, DatabusMessage> records = consumer.poll(1000);
            Date now = Calendar.getInstance().getTime();
            for (ConsumerRecord<Integer, DatabusMessage> record : records) {
                int kafkaKey = record.key();
                DatabusMessage kafkaValue = record.value();
                byte [] userPayload = kafkaValue.getPayload();
                String userValue = new String(userPayload);
                System.out.println(this.df.format(now) + " " + logTag + ":" +
                        " Received: {" + kafkaKey + ":" + userValue + "}" +
                        ", topic: " + record.topic() +
                        ", partition: " + record.partition() +
                        ", offset: " + record.offset());
            }
        }
        shutdownLatch.countDown();
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
