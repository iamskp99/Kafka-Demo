package com.shashankkumarpandey.kafka_producer.Service;

//import com.google.gson.JsonObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.shashankkumarpandey.kafka_producer.DTO.ProducerRequest;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class ProducerService {

    public Boolean pushPayloadToKafka(ProducerRequest requestObj){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Update with your Kafka broker address
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        // Send a message
        String topic = "temp_demo";
        String key = requestObj.name;
        ObjectMapper objectMapper = new ObjectMapper();

        ObjectNode jsonValue = objectMapper.createObjectNode();
        jsonValue.put("name", requestObj.name);
        jsonValue.put("score", requestObj.score);

        AtomicBoolean success = new AtomicBoolean(true);
        producer.send(new ProducerRecord<>(topic, key, jsonValue.toString()), (metadata, exception) -> {
            if (exception == null) {
                System.out.printf("Sent message to topic:%s partition:%d offset:%d%n",
                        metadata.topic(), metadata.partition(), metadata.offset());
            } else {
                exception.printStackTrace();
                success.set(false);
            }
        });

        producer.close();
        return success.get();
    }
}
