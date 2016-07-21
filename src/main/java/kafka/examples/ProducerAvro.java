package kafka.examples;

import kafka.examples.serializers.AvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProducerAvro extends Thread
{
    private final KafkaProducer<Integer, DatabusMessage> producer;
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
            RecordMetadata metadata;
            int kafkaKey = messageNo;

            Map<String, String> headers = new HashMap<>();
            headers.put("header1", "value1");
            String strValue = "Message_" + messageNo;
            byte [] userPayload = strValue.getBytes();
            DatabusMessage kafkaValue = new DatabusMessage(headers, userPayload);

            ProducerRecord producerRecord = new ProducerRecord<>(topic, kafkaKey, kafkaValue);

            long startTime = System.currentTimeMillis();
            if (isAsync)
            { // Send asynchronously
                producer.send(producerRecord, new DemoCallBack(logTag, startTime, messageNo, strValue));
            }
            else
            { // Send synchronously
                try
                {
                    Future<RecordMetadata> f = producer.send(producerRecord);
                    metadata = f.get();

                    long elapsedTime = System.currentTimeMillis() - startTime;
                    now = Calendar.getInstance().getTime();
                    System.out.println(this.df.format(now) + " " + logTag + ":" +
                            " Sent: {" + kafkaKey + ":" + strValue + "}" +
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