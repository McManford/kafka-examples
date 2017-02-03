package kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;


public class ProducerPartition extends Thread
{
    AtomicBoolean isRunning = new AtomicBoolean(true);
    CountDownLatch shutdownLatch = new CountDownLatch(1);

    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;
    private final int messagesToSend;
    private final DateFormat df;
    private final String logTag;

    public ProducerPartition(Properties props, Boolean isAsync, int messagesToSend)
    {
        logTag = "Producer1";

        producer = new KafkaProducer<>(props);
        this.topic = props.getProperty("topic");
        this.isAsync = isAsync;
        this.messagesToSend = messagesToSend;
        this.df = new SimpleDateFormat("HH:mm:ss");
    }

    public void run() {

        Date now = Calendar.getInstance().getTime();
        System.out.println(this.df.format(now) + " " + logTag + ": Mode: " + (isAsync ? "Async" : "Sync"));

        List<PartitionInfo> partitionInfoList = producer.partitionsFor(topic);
        for (PartitionInfo partInfo : partitionInfoList) {
            System.out.println(this.df.format(now) + " " + logTag + ": Partition: " + partInfo.partition());
            System.out.println(this.df.format(now) + " " + logTag + ": Leader Id: " + partInfo.leader().id());
            // rack added in 0.10.0.1
            //System.out.println(this.df.format(now) + " " + logTag + ":Leader Rack: " + partInfo.leader().rack());
        }

        int messageNo = 1;
        while (isRunning.get())
        {
            RecordMetadata metadata = null;
            int messageKey = messageNo;
            String messageValue = "Message_" + messageNo;
            long startTime = System.currentTimeMillis();

            Integer partition = 8;

            if (isAsync)
            { // Send asynchronously
                producer.send(new ProducerRecord<Integer, String>(topic, partition, messageNo, messageValue),
                        new DemoCallBack(logTag, startTime, messageNo, messageValue));
            }
            else
            { // Send synchronously
                try
                {
                    metadata = producer.send(new ProducerRecord<Integer, String>(topic, partition, messageNo, messageValue)).get();

                    long elapsedTime = System.currentTimeMillis() - startTime;
                    now = Calendar.getInstance().getTime();
                    System.out.println(this.df.format(now) +
                            " Producer1: Sent: {" + messageKey + ":" + messageValue + "}" +
                            ", topic: " + metadata.topic() +
                            ", partition: " + metadata.partition() +
                            ", offset: " + metadata.offset() +
                            ", elapsedTime: " + elapsedTime + " ms");
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
                catch (ExecutionException e)
                {
                    System.out.println(this.df.format(now) + " " + logTag + ":" +
                            " ERROR sending message. " +
                            "{" + messageKey + ":" + messageValue + "}" +
                            " Ex: " + e.getMessage());
                    // e.printStackTrace();
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
