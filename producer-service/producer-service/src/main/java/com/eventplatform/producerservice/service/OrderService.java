package com.eventplatform.producerservice.service;

import com.eventplatform.producerservice.entity.Order;
import com.eventplatform.producerservice.entity.OutboxEvent;
import com.eventplatform.producerservice.producer.OrderProducer;
import com.eventplatform.producerservice.repository.OrderRepository;
import com.eventplatform.producerservice.repository.OutboxRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public OrderService(OrderRepository orderRepository,
                        OutboxRepository outboxRepository,
                        ObjectMapper objectMapper) {

        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }
    @Transactional
    public void createOrder(Order order) {

        orderRepository.save(order);

        try {

            String payload = objectMapper.writeValueAsString(order);

            OutboxEvent event = new OutboxEvent();
            event.setEventType("ORDER_CREATED");
            event.setPayload(payload);

            outboxRepository.save(event);

        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize order.", e);
        }
    }


}