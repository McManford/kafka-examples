package kafka.examples;


public class ConsumerProducerDemo2
{
    public static void main(String[] args)
    {
        boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
        int messagesToProduce = args.length == 0 ? -1 : Integer.parseInt(args[1].trim());

        System.out.println("ConsumerProducerDemo2");

        KafkaProperties kprops = new KafkaProperties();

        Consumer1 consumer1Thread = new Consumer1(kprops);
        consumer1Thread.start();

        Consumer2 consumer2Thread = new Consumer2(kprops);
        consumer2Thread.start();

        Consumer3 consumer3Thread = new Consumer3(kprops);
        consumer3Thread.start();

        try {
            Thread.sleep(0);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Producer1 producerThread = new Producer1(kprops, isAsync, messagesToProduce);
        producerThread.start();
    }
}
