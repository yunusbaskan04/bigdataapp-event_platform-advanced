package com.eventplatform.consumerservice.repository;

import com.eventplatform.consumerservice.entity.Order;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, Long> {

}