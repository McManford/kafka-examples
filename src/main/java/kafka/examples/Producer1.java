package kafka.examples;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;

public class Producer1 extends Thread
{
    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;
    private final int messagesToSend;
    private final DateFormat df;
    private final String logTag;

    public Producer1(String topic, Boolean isAsync, int messagesToSend)
    {
        logTag = "Producer1";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "DemoProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<Integer, String>(props);
        this.topic = topic;
        this.isAsync = isAsync;
        this.messagesToSend = messagesToSend;
        this.df = new SimpleDateFormat("HH:mm:ss");
    }
    public void run() {

        Date now = Calendar.getInstance().getTime();
        System.out.println(this.df.format(now) + " " + logTag + ": Mode: " + (isAsync ? "Async" : "Sync"));

        List<PartitionInfo> partitionInfoList = producer.partitionsFor(topic);
        for (PartitionInfo partInfo : partitionInfoList) {
            System.out.println(this.df.format(now) + " " + logTag + ": Rack: " + partInfo.leader().rack());
        }

        int messageNo = 1;
        while(true)
        {
            RecordMetadata metadata = null;
            int messageKey = messageNo;
            String messageValue = "Message_" + messageNo;
            long startTime = System.currentTimeMillis();
            if (isAsync)
            { // Send asynchronously
                producer.send(new ProducerRecord<Integer, String>(topic, messageNo, messageValue),
                        new DemoCallBack(startTime, messageNo, messageValue));
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
    }
}

class DemoCallBack implements Callback {

    private long startTime;
    private int messageKey;
    private String messageValue;
    private final DateFormat df;

    public DemoCallBack(long startTime, int key, String message)
    {
        this.startTime = startTime;
        this.messageKey = key;
        this.messageValue = message;
        this.df = new SimpleDateFormat("HH:mm:ss");
    }

    /**
      * A callback method the user can implement to provide asynchronous handling of request completion. This method will
      * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
      * non-null.
      *
      * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
      *                  occurred.
      * @param exception The exception thrown during processing of this record. Null if no error occurred.
    */
    public void onCompletion(RecordMetadata metadata, Exception exception)
    {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null)
        {
            Date now = Calendar.getInstance().getTime();
            System.out.println(this.df.format(now) +
                    " Producer1: Sent: {" + messageKey + ":" + messageValue + "}" +
                    ", partition(" + metadata.partition() + ")" +
                    ", offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        }
        else
        {
            System.out.println("Producer1: ERROR sending message. Ex: " + exception.getMessage());
            exception.printStackTrace();
        }
    }
}
