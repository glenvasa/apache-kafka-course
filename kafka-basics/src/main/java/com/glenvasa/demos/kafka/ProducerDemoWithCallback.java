package com.glenvasa.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());


    public static void main(String[] args) {
        log.info("I am a Kafka Producer");

        // create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        // create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);



       for(int i=0; i<10; i++){
           // create a Producer record
           ProducerRecord<String, String> producerRecord =
                   new ProducerRecord<>("demo_java", "Producer Demo With Callback Message " + i);

           // send the data - async operation
           producer.send(producerRecord, new Callback() {
               @Override
               public void onCompletion(RecordMetadata metadata, Exception e) {
                   // executes everytime a record is successfully sent or an exception is thrown
                   if (e == null) {
                       // the record was successfully sent
                       log.info("Received new metadata/ \n" +
                               "Topic: " + metadata.topic() + "\n" +
                               "Partition: " + metadata.partition() + "\n" +
                               "Offset: " + metadata.offset() + "\n" +
                               "Timestamp: " + metadata.timestamp());
                   } else {
                       log.error("Error while producing: ", e);
                   }
               }
           });

           // this puts enough of a pause between records sent so that default partitioned among all partitions and not batched / sticky partitioned
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

       }


        // flush data - synchronous - tells program to block until all data sent is received by Kafka
        producer.flush();

        // close producer
        producer.close();

    }

}
