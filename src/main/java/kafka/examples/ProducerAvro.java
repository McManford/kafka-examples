package kafka.examples;

import kafka.examples.serializers.AvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerAvro extends Thread
{
    private final KafkaProducer<Integer, Object> producer;
    private final String topic;
    private final Boolean isAsync;
    private final int messagesToSend;
    private final DateFormat df;
    private final String logTag;

    public ProducerAvro(KafkaProperties kprops, Boolean isAsync, int messagesToSend)
    {
        logTag = "ProducerAvro";

        Properties props = new Properties();
        props.put("bootstrap.servers", kprops.KAFKA_BOOTSTRAP_SERVERS);
        props.put("client.id", "DemoProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "kafka.examples.serializers.AvroSerializer");

        final IntegerSerializer keySerializer = new IntegerSerializer();
        //avroKeySerializer.configure(avroProps, true);
        final AvroSerializer avroValueSerializer = new AvroSerializer();
        //avroValueSerializer.configure(avroProps, false);

        producer = new KafkaProducer<>(props, keySerializer, avroValueSerializer);
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
        while(true)
        {
            RecordMetadata metadata = null;
            int messageKey = messageNo;
            String strValue = "Message_" + messageNo;
            byte [] binaryValue = strValue.getBytes();
            long startTime = System.currentTimeMillis();
            if (isAsync)
            { // Send asynchronously
                producer.send(new ProducerRecord<>(topic, messageNo, binaryValue),
                        new DemoCallBack(startTime, messageNo, strValue));
            }
            else
            { // Send synchronously
                try
                {
                    metadata = producer.send(new ProducerRecord<>(topic, messageNo, strValue)).get();

                    long elapsedTime = System.currentTimeMillis() - startTime;
                    now = Calendar.getInstance().getTime();
                    System.out.println(this.df.format(now) +
                            " Producer1: Sent: {" + messageKey + ":" + strValue + "}" +
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
