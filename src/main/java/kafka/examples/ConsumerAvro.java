package kafka.examples;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.examples.serializers.AvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;


public class ConsumerAvro extends Thread
{
    AtomicBoolean isRunning = new AtomicBoolean(true);
    CountDownLatch shutdownLatch = new CountDownLatch(1);

    private final KafkaConsumer<Integer, Object> consumer;
    private final String topic;
    private final DateFormat df;
    private final String logTag;

    public ConsumerAvro(KafkaProperties kprops)
    {
        logTag = "ConsumerAvro";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kprops.KAFKA_BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kprops.GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kprops.AUTO_OFFSET_RESET);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        final IntegerDeserializer keyDeserializer = new IntegerDeserializer();
        //keyDeserializer.configure(avroProps, true);
        final AvroDeserializer avroValueDeserializer = new AvroDeserializer();
        //avroValuedeserializer.configure(avroProps, false);

        consumer = new KafkaConsumer<>(props, keyDeserializer, avroValueDeserializer);
        this.topic = kprops.TOPIC;
        this.df = new SimpleDateFormat("HH:mm:ss");

        consumer.subscribe(Collections.singletonList(this.topic));
    }

    public void doWork() {

    }

    public void run() {
        while (isRunning.get()) {
            //System.out.println(logTag + ": Doing work...");

            ConsumerRecords<Integer, Object> records = consumer.poll(1000);
            Date now = Calendar.getInstance().getTime();
            for (ConsumerRecord<Integer, Object> record : records) {
                System.out.println(this.df.format(now) + " " + logTag +
                        ": Received: " + record.value() + ", offset(" + record.offset() + ")");
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
