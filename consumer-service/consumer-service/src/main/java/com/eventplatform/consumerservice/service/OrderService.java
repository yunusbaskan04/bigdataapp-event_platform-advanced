package com.eventplatform.consumerservice.service;

import com.eventplatform.consumerservice.entity.Order;
import com.eventplatform.consumerservice.repository.OrderRepository;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    private final OrderRepository orderRepository;

    public OrderService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    public void save(Order order) {

        // İleride business logic buraya gelecek.

        orderRepository.save(order);
    }
}