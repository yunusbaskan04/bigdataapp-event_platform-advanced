package com.eventplatform.producerservice.service;

import com.eventplatform.producerservice.producer.OrderProducer;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    private final OrderProducer orderProducer;

    public OrderService(OrderProducer orderProducer) {
        this.orderProducer = orderProducer;
    }

    public void createOrder(String order) {

        orderProducer.send(order);

    }
}