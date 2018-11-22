package com.marina.simpletest.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {

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
            //create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world " + i);

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
            });
        }

        //flush data
        producer.flush();

        //flush and close producer
        producer.close();
    }
}
