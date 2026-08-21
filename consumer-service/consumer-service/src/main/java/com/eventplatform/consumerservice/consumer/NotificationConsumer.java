package com.eventplatform.consumerservice.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class NotificationConsumer {

    private final ObjectMapper objectMapper;

    public NotificationConsumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "orders", groupId = "notification-group")
    public void consumeNotification(
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) throws Exception {

        if (record.value() == null || record.value().isBlank()) {
            throw new IllegalArgumentException("Received empty/blank record payload in NotificationConsumer");
        }

        JsonNode jsonNode = objectMapper.readTree(record.value());
        String product = jsonNode.has("product") ? jsonNode.get("product").asText() : "Unknown Product";
        String orderId = jsonNode.has("orderId") ? jsonNode.get("orderId").asText() : "N/A";

        System.out.println("📧 [NotificationService] Sent email confirmation for Order #" + orderId + " (" + product + ")");

        acknowledgment.acknowledge();
    }
}
