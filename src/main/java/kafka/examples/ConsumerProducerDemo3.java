package kafka.examples;


public class ConsumerProducerDemo3
{
    public static void main(String[] args)
    {
        boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
        int messagesToProduce = args.length == 0 ? -1 : Integer.parseInt(args[1].trim());

        System.out.println("ConsumerProducerDemo3");

        KafkaProperties kprops = new KafkaProperties();

        ConsumerAssign1 consumer1Thread = new ConsumerAssign1(kprops);
        consumer1Thread.start();

        ConsumerAssign2 consumer2Thread = new ConsumerAssign2(kprops);
        consumer2Thread.start();

        ConsumerAssign3 consumer3Thread = new ConsumerAssign3(kprops);
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
