package kafka.examples;


public class ConsumerMain
{
    public static void main(String[] args)
    {
        System.out.println("ConsumerMain");

        KafkaProperties kprops = new KafkaProperties();

        Consumer1 consumerThread = new Consumer1(kprops);
        consumerThread.start();
    }
}
