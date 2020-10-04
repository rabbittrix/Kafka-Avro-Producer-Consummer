package com.jrsf;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaAvroProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.RETRIES_CONFIG, "10");
        properties.put("schema.registry.url", "http://localhost:8081");

        try (KafkaProducer<String, Customer> producer = new KafkaProducer<>(properties)) {
            String topic = "customer-avro";
            Customer customer = Customer.newBuilder()
                    .setIdPainel(200)
                    .setIdEventLog(1)
                    .setEventState("BT")
                    .setEventDescription("Low tension")
                    .setPainelDescription("EDP-VT292")
                    .setEventTimestamp("Check the panel")
                    .build();
            ProducerRecord<String, Customer> record = new ProducerRecord<>(topic, customer);

            producer.send(record, (metadata, e) -> {
                if (e == null) {
                    System.out.println("Success...!");
                    System.out.println(metadata.toString());
                } else {
                    e.printStackTrace();
                }
            });
            producer.flush();
        }

    }
}
