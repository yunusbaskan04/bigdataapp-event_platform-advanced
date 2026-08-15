package com.eventplatform.producerservice.repository;

import com.eventplatform.producerservice.entity.Order;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, Long> {

}