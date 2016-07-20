package kafka.examples;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Consumer1 extends Thread
{
  private final KafkaConsumer<Integer, String> consumer;
  private final String topic;
  private final DateFormat df;
  private final String logTag;

  public Consumer1(String topic)
  {
    logTag = "Consumer1";

    KafkaProperties kprops = new KafkaProperties();

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, kprops.GROUP_ID);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kprops.AUTO_OFFSET_RESET);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

    consumer = new KafkaConsumer<>(props);
    this.topic = topic;
    this.df = new SimpleDateFormat("HH:mm:ss");

    consumer.subscribe(Collections.singletonList(this.topic));
  }

  public void doWork() {
    //System.out.println(logTag + ": Doing work...");

    ConsumerRecords<Integer, String> records = consumer.poll(1000);
    Date now = Calendar.getInstance().getTime();
    for (ConsumerRecord<Integer, String> record : records) {
      System.out.println(this.df.format(now) + " " + logTag +
              ": Received: " + record.value() + ", offset(" + record.offset() + ")");
    }
  }

  public void run() {
    while (true) {
      doWork();
    }
  }

}