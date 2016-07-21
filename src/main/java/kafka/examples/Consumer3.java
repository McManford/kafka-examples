package kafka.examples;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;

public class Consumer3 extends Thread
{
  private final KafkaConsumer<Integer, String> consumer;
  private final String topic;
  private final DateFormat df;
  private final String logTag;

  public Consumer3(KafkaProperties kprops)
  {
    logTag = "Consumer3";

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

    consumer = new KafkaConsumer<>(props);
    this.topic = kprops.TOPIC;
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