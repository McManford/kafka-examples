package kafka.examples;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;

public class Producer1 extends Thread
{
    AtomicBoolean isRunning = new AtomicBoolean(true);
    CountDownLatch shutdownLatch = new CountDownLatch(1);

    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;
    private final int messagesToSend;
    private final DateFormat df;
    private final String logTag;

    public Producer1(KafkaProperties kprops, Boolean isAsync, int messagesToSend)
    {
        logTag = "Producer1";

        Properties props = new Properties();
        props.put("bootstrap.servers", kprops.KAFKA_BOOTSTRAP_SERVERS);
        props.put("client.id", "DemoProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<Integer, String>(props);
        this.topic = kprops.TOPIC;
        this.isAsync = isAsync;
        this.messagesToSend = messagesToSend;
        this.df = new SimpleDateFormat("HH:mm:ss");
    }

    public void run() {

        Date now = Calendar.getInstance().getTime();
        System.out.println(this.df.format(now) + " " + logTag + ": Mode: " + (isAsync ? "Async" : "Sync"));

        //List<PartitionInfo> partitionInfoList = producer.partitionsFor(topic);
        //for (PartitionInfo partInfo : partitionInfoList) {
        //    System.out.println(this.df.format(now) + " " + logTag + ": Rack: " + partInfo.leader().rack());
        //}

        int messageNo = 1;
        while (isRunning.get())
        {
            RecordMetadata metadata = null;
            int messageKey = messageNo;
            String messageValue = "Message_" + messageNo;
            long startTime = System.currentTimeMillis();
            if (isAsync)
            { // Send asynchronously
                producer.send(new ProducerRecord<Integer, String>(topic, messageNo, messageValue),
                        new DemoCallBack(logTag, startTime, messageNo, messageValue));
            }
            else
            { // Send synchronously
                try
                {
                    metadata = producer.send(new ProducerRecord<Integer, String>(topic, messageNo, messageValue)).get();

                    long elapsedTime = System.currentTimeMillis() - startTime;
                    now = Calendar.getInstance().getTime();
                    System.out.println(this.df.format(now) +
                            " Producer1: Sent: {" + messageKey + ":" + messageValue + "}" +
                            ", partition(" + metadata.partition() + ")" +
                            ", offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
                catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }

            ++messageNo;
            if (messagesToSend != -1 && messageNo > messagesToSend)
                break;

            try
            {
                Thread.sleep(500);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
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
