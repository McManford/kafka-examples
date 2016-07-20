package kafka.examples;


public class ConsumerProducerDemo1 {
    public static void main(String[] args)
    {
        boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
        int messagesToProduce = args.length == 0 ? -1 : Integer.parseInt(args[1].trim());

        Consumer1 consumerThread = new Consumer1(KafkaProperties.TOPIC);
        consumerThread.start();

        try {
            Thread.sleep(0);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        Producer1 producerThread = new Producer1(KafkaProperties.TOPIC, isAsync, messagesToProduce);
        producerThread.start();


    }
}
