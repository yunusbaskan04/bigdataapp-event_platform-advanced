package com.eventplatform.producerservice.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String message) {

        kafkaTemplate.send("orders", message)
                .whenComplete((result, ex) -> {

                    if (ex == null) {
                        System.out.println("Mesaj gönderildi.");
                        System.out.println(result.getRecordMetadata());
                    } else {
                        ex.printStackTrace();
                    }

                });

    }
}