package kafka.examples;


public class ConsumerProducerAvroDemo1 {
    public static void main(String[] args)
    {
        boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
        int messagesToProduce = args.length == 0 ? -1 : Integer.parseInt(args[1].trim());

        System.out.println("ConsumerProducerAvroDemo1");

        KafkaProperties kprops = new KafkaProperties();

        ConsumerAvro consumerThread = new ConsumerAvro(kprops);
        consumerThread.start();

        try {
            Thread.sleep(0);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ProducerAvro producerThread = new ProducerAvro(kprops, isAsync, messagesToProduce);
        producerThread.start();
    }
}
