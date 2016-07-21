package kafka.examples;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;


public class DemoCallBack implements Callback {

    private String logTag;
    private long startTime;
    private int messageKey;
    private String messageValue;
    private final DateFormat df;

    public DemoCallBack(String logTag, long startTime, int key, String message)
    {
        this.logTag = logTag;
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
        Date now = Calendar.getInstance().getTime();
        if (metadata != null)
        {

            System.out.println(this.df.format(now) + " " + logTag + ":" +
                    " Sent: {" + messageKey + ":" + messageValue + "}" +
                    ", partition(" + metadata.partition() + ")" +
                    ", offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        }
        else
        {
            System.out.println(this.df.format(now) + " " + logTag + ":" +
                    " ERROR sending message. Ex: " + exception.getMessage());
            exception.printStackTrace();
        }
    }
}
