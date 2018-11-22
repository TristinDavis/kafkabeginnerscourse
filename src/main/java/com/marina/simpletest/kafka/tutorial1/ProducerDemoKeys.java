package com.marina.simpletest.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    static Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        String bootstrapServers = "127.0.0.1:9092";

        //create Producer properties
        //check producer properties in https://kafka.apache.org/documentation/#producerconfigs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //how it can be serialized to bytes
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        //<String, String> --> key and value are strings
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0; i<10; i++) {
            String topic = "topic-2-3-partitions";
            String value = "hello world " + i;
            String key = "key_" + (i%3);

            logger.info("key: " + key);
            //create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            //send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //it runs every time a record is sent successfully or it happens an exception
                    if (e == null) {
                        StringBuilder sb = new StringBuilder("\nRecord was successfully sent. Received new metadata:");
                        sb.append("\ntopic: ");
                        sb.append(recordMetadata.topic());
                        sb.append("\npartition: ");
                        sb.append(recordMetadata.partition());
                        sb.append("\noffset: ");
                        sb.append(recordMetadata.offset());
                        sb.append("\ntimestamp: ");
                        sb.append(recordMetadata.timestamp());

                        logger.info(sb.toString());

                    } else {
                        logger.error("Error while sending record: ", e);
                    }

                }
            }).get();
        }

        //flush data
        producer.flush();

        //flush and close producer
        producer.close();
    }
}
