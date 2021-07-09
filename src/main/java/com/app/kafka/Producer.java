package com.app.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        System.out.println("Kafka Module Start");

        //Create Properties Object For Producer
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        System.out.println( StringSerializer.class.getName());
        System.out.println(ProducerConfig.PARTITIONER_CLASS_CONFIG);
        System.out.println(ProducerConfig.COMPRESSION_TYPE_CONFIG);
        System.out.println(ProducerConfig.BATCH_SIZE_CONFIG);
        System.out.println(ProducerConfig.SEND_BUFFER_CONFIG);
        //Create Kafka Producer
        final KafkaProducer<String,String> producer = new KafkaProducer<String, String>(prop);

        //Create Producer Record
        ProducerRecord<String, String> record = new ProducerRecord<>("firstTopic", "Message From Java 01072021");

        //Send Data
        producer.send(record);

        //Flush and Close
        producer.flush();
        producer.close();

    }
}
