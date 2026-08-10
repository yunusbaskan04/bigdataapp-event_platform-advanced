package com.eventplatform.producerservice.controller;

import com.eventplatform.producerservice.service.OrderService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public String createOrder(@RequestBody String order) {

        orderService.createOrder(order);

        return "Order sent to Kafka";

    }
}