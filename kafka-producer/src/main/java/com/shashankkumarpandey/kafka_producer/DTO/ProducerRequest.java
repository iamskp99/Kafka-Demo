package com.shashankkumarpandey.kafka_producer.DTO;

public class ProducerRequest {
    public String name;
    public Integer score;

    public void setProducerPayload(String name,Integer score) {
        this.name = name;
        this.score = score;
    }
}
