/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
