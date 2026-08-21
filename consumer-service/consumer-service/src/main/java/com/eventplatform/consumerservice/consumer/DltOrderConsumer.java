package com.eventplatform.consumerservice.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class DltOrderConsumer {

    @KafkaListener(topics = "orders-dlt", groupId = "dlt-order-group")
    public void consumeDlt(
            ConsumerRecord<String, String> record,
            @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exceptionMessage,
            @Header(name = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false) String originalTopic,
            @Header(name = KafkaHeaders.DLT_ORIGINAL_OFFSET, required = false) Long originalOffset,
            Acknowledgment acknowledgment) {

        System.err.println("------------------------------------------------------------------");
        System.err.println("🚨 ALERT: Poison Pill / Failed Message Routed to DLT!");
        System.err.println("Payload: " + record.value());
        System.err.println("Original Topic: " + originalTopic + " | Original Offset: " + originalOffset);
        System.err.println("Exception Message: " + exceptionMessage);
        System.err.println("------------------------------------------------------------------");

        acknowledgment.acknowledge();
    }
}
