package kafka.examples;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


public class DemoCallBack implements Callback {

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
