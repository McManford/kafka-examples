package kafka.examples;


public class ProducerMain
{
    public static void main(String[] args)
    {
        boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
        int messagesToProduce = args.length == 0 ? -1 : Integer.parseInt(args[1].trim());

        System.out.println("ProducerMain");

        KafkaProperties kprops = new KafkaProperties();

        Producer1 producerThread = new Producer1(kprops, isAsync, messagesToProduce);
        producerThread.start();
    }
}
