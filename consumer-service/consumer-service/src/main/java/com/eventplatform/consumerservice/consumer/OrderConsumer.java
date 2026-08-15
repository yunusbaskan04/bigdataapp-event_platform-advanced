package com.eventplatform.consumerservice.consumer;

import com.eventplatform.consumerservice.entity.Order;
import com.eventplatform.consumerservice.service.OrderService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {

    private final ObjectMapper objectMapper;
    private final OrderService orderService;

    public OrderConsumer(ObjectMapper objectMapper,
                         OrderService orderService) {
        this.objectMapper = objectMapper;
        this.orderService = orderService;
    }

    @KafkaListener(topics = "orders")
    public void consume(ConsumerRecord<String, String> record,
                        Acknowledgment acknowledgment) {

        try {

            Order order = objectMapper.readValue(
                    record.value(),
                    Order.class
            );

            orderService.save(order);

            acknowledgment.acknowledge();

            System.out.println("Order saved successfully.");

        } catch (Exception e) {

            System.out.println("Error : " + e.getMessage());

            throw new RuntimeException(e);
        }
    }
}