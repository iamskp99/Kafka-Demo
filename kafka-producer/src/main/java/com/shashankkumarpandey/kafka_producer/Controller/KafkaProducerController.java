package com.shashankkumarpandey.kafka_producer.Controller;


import com.shashankkumarpandey.kafka_producer.DTO.ProducerRequest;
import com.shashankkumarpandey.kafka_producer.Service.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/postKafka")
public class KafkaProducerController {
    @Autowired
    ProducerService producerService;

    @PostMapping("/push")
    public ResponseEntity<String> pushKafka(@RequestBody Map<String, Object> requestData){
        ProducerRequest internalRequestObj = new ProducerRequest();

        String name = (String) requestData.get("name");
        Integer score = (Integer) requestData.get("score");

        internalRequestObj.setProducerPayload(name,score);

        Boolean resp = producerService.pushPayloadToKafka(internalRequestObj);

        if(resp == Boolean.TRUE){
            return ResponseEntity.ok("Kafka Payload Pushed !!!!");
        }else{
            return ResponseEntity.ok("Could not push to Kafka !!!!");
        }
    }
}
